// Copyright 2020 The Mangos Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use file except in compliance with the License.
// You may obtain a copy of the license at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package transport

import (
	"net"

	"go.nanomsg.org/mangos/v3"
)

// NewConnPipeIPC allocates a new Pipe using the IPC exchange protocol.
func NewConnPipeIPC(c net.Conn, proto ProtocolInfo) ConnPipe {
	p := &connipc{
		conn: conn{
			c:       c,
			proto:   proto,
			options: make(map[string]interface{}),
			maxrx:   0,
		},
	}
	p.options[mangos.OptionMaxRecvSize] = 0
	p.options[mangos.OptionLocalAddr] = c.LocalAddr()
	p.options[mangos.OptionRemoteAddr] = c.RemoteAddr()
	return p
}
