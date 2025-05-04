//go:build windows

package jpyexec

import (
	"net"
	"os"
	"sync"

	"github.com/Microsoft/go-winio"
	"github.com/Microsoft/go-winio/pkg/guid"

	"k8s.io/klog/v2"

	"github.com/pkg/errors"
	"io"
	"encoding/gob"
	
	"github.com/janpfeifer/gonb/gonbui/protocol"
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
	
	go func() {
		klog.V(2).Infof("Opening named pipeReader in %q", exec.namedPipeReaderPath)
		if exec.isDone {
			// In case program execution interrupted early.
			return
		}
		// Notice that opening pipeReader below blocks, until the other end
		// (the go program being executed) opens it as well.
		w, err := winio.ListenPipe(exec.namedPipeReaderPath, nil)
		exec.conn, err = w.Accept()
		if err != nil {
			klog.Warningf("Failed to open pipe (Mkfifo) %q for reading: %+v", exec.namedPipeReaderPath, err)
			return
		}
		klog.V(2).Infof("Opened named pipeReader in %q", exec.namedPipeReaderPath)
		muFifo.Lock()
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
	decoder := gob.NewDecoder(exec.conn)
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

// openPipeWriter opens `exec.namedPipeWriterPath` and handles its proper closing, and removal of
// the named pipe when program execution is finished.
//
// The doneChan is listened to: when it is closed, it will trigger the listener goroutine to close the pipe,
// remove it and quit.
func (exec *Executor) openPipeWriter() {
	// Synchronize pipe: if it's not opened by the program being executed,
	// we have to open it ourselves for writing, to avoid blocking
	// `os.Open` (it waits the other end of the fifo to be opened before returning).
	// See discussion in:
	// https://stackoverflow.com/questions/75255426/how-to-interrupt-a-blocking-os-open-call-waiting-on-a-fifo-in-go
	var muFifo sync.Mutex
	fifoOpened := false

	go func() {
		// Clean up after program is over, there are two scenarios:
		// 1. The executed program opened the pipe: then we just remove the pipePath.
		// 2. The executed program never opened the pipe: then the other end (goroutine
		//    below) will be forever blocked on os.Open call.
		<-exec.doneChan
		muFifo.Lock()
		if !fifoOpened {
			r, err := os.OpenFile(exec.namedPipeWriterPath, os.O_RDONLY, 0600)
			if err == nil {
				// Closing it allows the open of the pipe for writing (below) to unblock.
				_ = r.Close()
			}
		}
		muFifo.Unlock()
		_ = os.Remove(exec.namedPipeWriterPath)
	}()

	go func() {
		klog.V(2).Infof("Opening named pipeWriter in %q", exec.namedPipeWriterPath)
		if exec.isDone {
			// In case program execution interrupted early.
			klog.Warningf("Opening of NamedPipeWriter in %q failed, since program already stopped/crashed", exec.namedPipeWriterPath)
			return
		}
		// Notice that opening the pipe below blocks, until the other end (the go program being executed) opens it
		// as well.
		f, err := os.OpenFile(exec.namedPipeWriterPath, os.O_WRONLY, 0600)
		if err != nil {
			klog.Warningf("Failed to open pipe (Mkfifo) %q for writing: %+v", exec.namedPipeWriterPath, err)
			return
		}
		klog.V(2).Infof("Opened named pipeWriter in %q", exec.namedPipeWriterPath)
		muFifo.Lock()
		exec.pipeWriter = f
		fifoOpened = true
		defer muFifo.Unlock()

		// Start polling of the pipeReader.
		go exec.pollPipeWriterFifo()

		// Wait program execution to finish to close the writer (file and fifo).
		<-exec.doneChan
		close(exec.PipeWriterFifo)
		_ = exec.pipeWriter.Close()
		_ = os.Remove(exec.namedPipeWriterPath)
	}()
}

// pollPipeWriterFifo polls messages from `Executor.PipeWriterFifo` and encodes them to
// the named pipe writer.
func (exec *Executor) pollPipeWriterFifo() {
	encoder := gob.NewEncoder(exec.pipeWriter)
	klog.V(2).Infof("jpyexec: pollPipeWriterFifo() listening to requests.")
	for msg := range exec.PipeWriterFifo {
		if klog.V(2).Enabled() {
			klog.Infof("jpyexec: encoding %+v to named pipe to cell program", msg)
		}
		err := encoder.Encode(msg)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrClosedPipe) || errors.Is(err, os.ErrClosed) {
			return
		} else if err != nil {
			klog.Infof("while writing to cell program, failed to encode message %+v. "+
				"Communication with cell program broken, widgets won't work properly. "+
				"You can try re-executing the cell. Error: %+v", msg, err)
			return
		}
	}
	klog.V(2).Infof("jpyexec: pollPipeWriterFifo() closed.")
}
