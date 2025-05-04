//go:build windows

package jpyexec

import (
	"net"
	"os"
	"sync"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/go-winio/pkg/guid"

	"k8s.io/klog/v2"
)

func (exec *Executor) createTmpFifo() (string, error) {
	// Create a temporary file name.
	g, _ := guid.NewV4()
	pipePath := `\\.\pipe\gonb_pipe` + g.String()
	return pipePath, nil
}


// openPipeReader opens `exec.namedPipeReaderPath` and handles its proper closing, and removal of
// the named pipe when program execution is finished.
//
// The doneChan is listened to: when it is closed, it will trigger the listener goroutine to close the pipe,
// remove it and quit.
func (exec *Executor) openPipeReader() {
	// Synchronize pipe: if it's not opened by the program being executed,
	// we have to open it ourselves for writing, to avoid blocking
	// `os.Open` (it waits the other end of the fifo to be opened before returning).
	// See discussion in:
	// https://stackoverflow.com/questions/75255426/how-to-interrupt-a-blocking-os-open-call-waiting-on-a-fifo-in-go
	var muFifo sync.Mutex
	fifoOpenedForReading := false

	var w net.Listener

	go func() {
		// Clean up after program is over, there are two scenarios:
		// 1. The executed program opened the pipe: then we just remove the pipePath.
		// 2. The executed program never opened the pipe: then the other end (goroutine
		//    below) will be forever blocked on os.Open call.
		<-exec.doneChan
		muFifo.Lock()
		if !fifoOpenedForReading {
			// w, err := os.OpenFile(exec.namedPipeReaderPath, os.O_WRONLY, 0600)
			w, err := winio.ListenPipe(exec.namedPipeReaderPath, nil)
			if err == nil {
				// Closing it allows the open of the pipe for reading (below) to unblock.
				_ = w.Close()
			}
		}
		muFifo.Unlock()
		_ = os.Remove(exec.namedPipeReaderPath)
	}()

	go func() {
		klog.V(2).Infof("Opening named pipeReader in %q", exec.namedPipeReaderPath)
		if exec.isDone {
			// In case program execution interrupted early.
			return
		}
		// Notice that opening pipeReader below blocks, until the other end
		// (the go program being executed) opens it as well.
		var err error
		exec.conn, err = w.Accept()
		if err != nil {
			klog.Warningf("Failed to open pipe (Mkfifo) %q for reading: %+v", exec.namedPipeReaderPath, err)
			return
		}
		klog.V(2).Infof("Opened named pipeReader in %q", exec.namedPipeReaderPath)
		muFifo.Lock()
		fifoOpenedForReading = true
		defer muFifo.Unlock()

		// Start polling of the pipeReader.
		go exec.pollNamedPipeReader()

		// Wait program execution to finish to close reader (in case it is not yet closed).
		<-exec.doneChan
		_ = exec.conn.Close()
	}()
}


// pollNamedPipeReader will continuously read for incoming requests with displaying content
// on the notebook or widgets updates.
func (exec *Executor) pollNamedPipeReader() {
	decoder := gob.NewDecoder(exec.pipeReader)
	for {
		data := &protocol.DisplayData{}
		err := decoder.Decode(data)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrClosed) {
			return
		} else if err != nil {
			klog.Infof("Named pipe: failed to parse message: %+v", err)
			return
		}

		// Special case for a request for input:
		if reqAny, found := data.Data[protocol.MIMEJupyterInput]; found {
			klog.V(2).Infof("Received InputRequest: %v", reqAny)
			req, ok := reqAny.(protocol.InputRequest)
			if !ok {
				exec.reportCellError(errors.Errorf(
					"A MIMEJupyterInput sent to GONB_PIPE without an associated protocol.InputRequest!? -- got (%T) %#v",
					reqAny, reqAny))
				continue
			}
			exec.dispatchInputRequest(&req)
			continue
		}

		// CommValue: update or read value in the front-end.
		if reqAny, found := data.Data[protocol.MIMECommValue]; found {
			req, ok := reqAny.(protocol.CommValue)
			if !ok {
				exec.reportCellError(errors.Errorf(
					"Invalid message sent in named pipes to GoNB from cell, "+
						"this may affect widgets communication -- "+
						"MIMECommValue sent to $GONB_PIPE_BACK without an associated `protocol.CommValue` "+
						"type, got %T instead", reqAny))
				continue
			}

			// Special addresses:
			if req.Address == protocol.GonbuiSyncAddress {
				syncId, ok := req.Value.(int)
				if !ok {
					klog.Errorf("comms: Receive Sync request with invalid value %+v. Communication with cell program may be left in an unusable state!", req)
					continue
				}
				klog.V(2).Infof("comms: Received Sync(%d) at %q, sending back ack", syncId, req.Address)
				// Acknowledge with a reply to the special address.
				exec.PipeWriterFifo <- &protocol.CommValue{
					Address: protocol.GonbuiSyncAckAddress,
					Value:   syncId,
				}
				continue
			}

			if exec.commsHandler == nil {
				klog.V(2).Infof("Received and dropped (no handler registered) CommValue: %+v", req)
			} else if req.Request {
				klog.V(2).Infof("ProgramReadValueRequest(%q) requested", req.Address)
				exec.commsHandler.ProgramReadValueRequest(req.Address)
			} else {
				klog.V(2).Infof("ProgramSendValueRequest(%q, %v) requested", req.Address, req.Value)
				exec.commsHandler.ProgramSendValueRequest(req.Address, req.Value)
			}
			continue
		}

		// ProgramSubscribeRequest: (un-)subscribe to address in the front-end.
		if reqAny, found := data.Data[protocol.MIMECommSubscribe]; found {
			req, ok := reqAny.(protocol.CommSubscription)
			if !ok {
				exec.reportCellError(errors.Errorf(
					"Invalid message sent in named pipes to GoNB from cell, "+
						"this may affect widgets communication -- "+
						"MIMECommSubscribe sent to $GONB_PIPE_BACK without an associated `protocol.CommSubscription` "+
						"type, got %T instead", reqAny))
				continue
			}
			if exec.commsHandler == nil {
				klog.V(2).Infof("Received and dropped (no handler registered) ProgramSubscribeRequest: %+v", req)
			} else if req.Unsubscribe {
				klog.V(2).Infof("ProgramUnsubscribeRequest(%q) requested", req.Address)
				exec.commsHandler.ProgramUnsubscribeRequest(req.Address)
			} else {
				klog.V(2).Infof("ProgramSubscribeRequest(%q) requested", req.Address)
				exec.commsHandler.ProgramSubscribeRequest(req.Address)
			}
			continue
		}

		// Otherwise, just display with the corresponding MIME type:
		exec.dispatchDisplayData(data)
	}
}