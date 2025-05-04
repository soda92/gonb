package jpyexec

// This file implements the polling on the $GONB_PIPE and $GONB_PIPE_WIDGETS named pipes created
// to receive information from the program being executed and to send information from the
// widgets.
//
// It has a protocol (defined under `gonbui/protocol`) to display rich content.

import (
	"encoding/gob"
	"fmt"
	"github.com/janpfeifer/gonb/gonbui/protocol"
	"github.com/janpfeifer/gonb/internal/kernel"
	"github.com/pkg/errors"
	"k8s.io/klog/v2"
)

func init() {
	// Register generic gob types we want to make sure are understood.
	gob.Register(map[string]any{})
	gob.Register([]string{})
	gob.Register([]any{})
}

// CommsHandler interface is used if Executor.UseNamedPipes is called, and a CommsHandler
// is provided.
//
// It is assumed there is at most one program being executed at a time. GoNB will never
// execute two cells simultaneously.
type CommsHandler interface {
	// ProgramStart is called when the program execution is about to start.
	// If program start failed (e.g.: during creation of pipes), ProgramStart may not be called,
	// and yet ProgramFinished is called.
	ProgramStart(exec *Executor)

	// ProgramFinished is called when the program execution finishes.
	// Notice this may be called even if ProgramStart has not been called, if the execution
	// failed during the creation of the various pipes.
	ProgramFinished()

	// ProgramSendValueRequest is called when the program requests a value to be sent to an address.
	ProgramSendValueRequest(address string, value any)

	// ProgramReadValueRequest handler.
	ProgramReadValueRequest(address string)

	// ProgramSubscribeRequest handler.
	ProgramSubscribeRequest(address string)

	// ProgramUnsubscribeRequest handler.
	ProgramUnsubscribeRequest(address string)
}

// PipeWriterFifoBufferSize is the number of CommValue messages that
// can be buffered when writing to the named pipe before dropping.
const PipeWriterFifoBufferSize = 128

// handleNamedPipes creates the named pipe and set up the goroutines to listen to them.
//
// TODO: make this more secure, maybe with a secret key also passed by the environment.
func (exec *Executor) handleNamedPipes() (err error) {
	exec.PipeWriterFifo = make(chan *protocol.CommValue, PipeWriterFifoBufferSize)

	// Create temporary named pipes in both directions.
	exec.namedPipeReaderPath, err = exec.createTmpFifo()
	if err != nil {
		return errors.Wrapf(err, "creating named pipe used to read from program %s", exec.cmd)
	}
	exec.namedPipeWriterPath, err = exec.createTmpFifo()
	if err != nil {
		return errors.Wrapf(err, "creating named pipe used to write to program %s", exec.cmd)
	}
	exec.cmd.Env = append(exec.cmd.Environ(),
		protocol.GONB_PIPE_ENV+"="+exec.namedPipeReaderPath,
		protocol.GONB_PIPE_BACK_ENV+"="+exec.namedPipeWriterPath)

	exec.openPipeReader()
	exec.openPipeWriter()
	return
}

// reportCellError reports error to both, the notebook and the standard logger (gonb's stderr).
func (exec *Executor) reportCellError(err error) {
	errStr := fmt.Sprintf("%+v", err) // Error with stack.
	klog.Errorf("%s", errStr)
	err = kernel.PublishWriteStream(exec.Msg, kernel.StreamStderr, "GoNB Error:\n"+errStr)
	if err != nil {
		klog.Errorf("%+v", errors.WithStack(err))
	}
}

// dispatchDisplayData received through the named pipe (`$GONB_PIPE`).
func (exec *Executor) dispatchDisplayData(data *protocol.DisplayData) {
	// Log info about what is being displayed.
	msgData := kernel.Data{
		Data:      make(kernel.MIMEMap, len(data.Data)),
		Metadata:  make(kernel.MIMEMap),
		Transient: make(kernel.MIMEMap),
	}
	for mimeType, content := range data.Data {
		msgData.Data[string(mimeType)] = content

		// Capture display data output, if requested.
		if exec.captureDisplayDataOutput != nil {
			str, ok := content.(string)
			if ok {
				_, err := exec.captureDisplayDataOutput.Write([]byte(str))
				if err != nil {
					klog.Errorf("failed to capture display data output: %v", err)
				}
			}
		}
	}

	if klog.V(1).Enabled() {
		kernel.LogDisplayData(msgData.Data)
	}
	for key, content := range data.Metadata {
		msgData.Metadata[key] = content
	}
	var err error
	if data.DisplayID != "" {
		msgData.Transient["display_id"] = data.DisplayID
		err = kernel.PublishUpdateDisplayData(exec.Msg, msgData)
	} else {
		err = kernel.PublishData(exec.Msg, msgData)
	}
	if err != nil {
		klog.Errorf("Failed to display data (ignoring): %v", err)
	}
}

// dispatchInputRequest uses the standard Jupyter input mechanism.
// It is fundamentally broken -- it locks the UI even if the program already stopped running --
// so we suggest using the `gonb/gonbui/widgets` API instead.
func (exec *Executor) dispatchInputRequest(req *protocol.InputRequest) {
	klog.V(2).Infof("Received InputRequest %+v", req)
	writeStdinFn := func(original, input *kernel.MessageImpl) error {
		content := input.Composed.Content.(map[string]any)
		value := content["value"].(string) + "\n"
		klog.V(2).Infof("stdin value: %q", value)
		go func() {
			exec.muDone.Lock()
			cmdStdin := exec.cmdStdin
			exec.muDone.Unlock()
			if exec.isDone {
				return
			}
			// Write concurrently, not to block, in case program doesn't
			// actually read anything from the stdin.
			_, err := cmdStdin.Write([]byte(value))
			if err != nil {
				// Could happen if something was not fully written, and channel was closed, in
				// which case it's ok.
				klog.Warningf("failed to write to stdin of cell: %+v", err)
			}
		}()
		return nil
	}
	err := exec.Msg.PromptInput(req.Prompt, req.Password, writeStdinFn)
	if err != nil {
		exec.reportCellError(err)
	}
}
