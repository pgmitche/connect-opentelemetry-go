// Copyright 2022 Buf Technologies, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package otelconnect

import (
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
	"sync"

	"github.com/bufbuild/connect-go"
)

var (
	recvSpanType = semconv.MessageTypeKey.String("RECEIVED")
	sendSpanType = semconv.MessageTypeKey.String("SENT")
)

type streamingClientInterceptor struct {
	connect.StreamingClientConn

	onClose func()

	mu             sync.Mutex
	requestClosed  bool
	responseClosed bool
	onCloseCalled  bool

	rcvMsgID  int
	sendMsgID int
	span      trace.Span
	state     *streamingState
}

func (s *streamingClientInterceptor) Receive(msg any) error {
	err := s.state.receive(msg, s.StreamingClientConn)
	s.rcvMsgID++
	recvSpanAttrs := []attribute.KeyValue{recvSpanType, semconv.MessageIDKey.Int(s.rcvMsgID)}
	if err == nil {
		if pmsg, ok := msg.(proto.Message); ok {
			size := proto.Size(pmsg)
			recvSpanAttrs = append(recvSpanAttrs, semconv.MessageUncompressedSizeKey.Int(size))
		}
	}
	s.span.AddEvent("message", trace.WithAttributes(recvSpanAttrs...))
	return err
}

func (s *streamingClientInterceptor) Send(msg any) error {
	err := s.state.send(msg, s.StreamingClientConn)
	s.sendMsgID++
	sendSpanAttrs := []attribute.KeyValue{sendSpanType, semconv.MessageIDKey.Int(s.sendMsgID)}
	if err == nil {
		if pmsg, ok := msg.(proto.Message); ok {
			size := proto.Size(pmsg)
			sendSpanAttrs = append(sendSpanAttrs, semconv.MessageUncompressedSizeKey.Int(size))
		}
	}
	s.span.AddEvent("message", trace.WithAttributes(sendSpanAttrs...))
	return err
}

func (s *streamingClientInterceptor) CloseRequest() error {
	err := s.StreamingClientConn.CloseRequest()
	s.mu.Lock()
	s.requestClosed = true
	shouldCall := s.responseClosed && !s.onCloseCalled
	if shouldCall {
		s.onCloseCalled = true
	}
	s.mu.Unlock()
	if shouldCall {
		s.onClose()
	}
	return err
}

func (s *streamingClientInterceptor) CloseResponse() error {
	err := s.StreamingClientConn.CloseResponse()
	s.mu.Lock()
	s.responseClosed = true
	shouldCall := s.requestClosed && !s.onCloseCalled
	if shouldCall {
		s.onCloseCalled = true
	}
	s.mu.Unlock()
	if shouldCall {
		s.onClose()
	}
	return err
}

type errorStreamingClientInterceptor struct {
	connect.StreamingClientConn

	err error
}

func (e *errorStreamingClientInterceptor) Send(any) error {
	return e.err
}

func (e *errorStreamingClientInterceptor) CloseRequest() error {
	if err := e.StreamingClientConn.CloseRequest(); err != nil {
		return fmt.Errorf("%w %s", err, e.err.Error())
	}
	return e.err
}

func (e *errorStreamingClientInterceptor) Receive(any) error {
	return e.err
}

func (e *errorStreamingClientInterceptor) CloseResponse() error {
	if err := e.StreamingClientConn.CloseResponse(); err != nil {
		return fmt.Errorf("%w %s", err, e.err.Error())
	}
	return e.err
}

type streamingHandlerInterceptor struct {
	connect.StreamingHandlerConn
	span      trace.Span
	state     *streamingState
	rcvMsgID  int
	sendMsgID int
}

func (p *streamingHandlerInterceptor) Receive(msg any) error {
	err := p.state.receive(msg, p.StreamingHandlerConn)
	p.rcvMsgID++
	recvSpanAttrs := []attribute.KeyValue{recvSpanType, semconv.MessageIDKey.Int(p.rcvMsgID)}
	if err == nil {
		if pmsg, ok := msg.(proto.Message); ok {
			size := proto.Size(pmsg)
			recvSpanAttrs = append(recvSpanAttrs, semconv.MessageUncompressedSizeKey.Int(size))
		}
	}
	p.span.AddEvent("message", trace.WithAttributes(recvSpanAttrs...))
	return err
}

func (p *streamingHandlerInterceptor) Send(msg any) error {
	err := p.state.send(msg, p.StreamingHandlerConn)
	p.sendMsgID++
	sendSpanAttrs := []attribute.KeyValue{sendSpanType, semconv.MessageIDKey.Int(p.sendMsgID)}
	if err == nil {
		if pmsg, ok := msg.(proto.Message); ok {
			size := proto.Size(pmsg)
			sendSpanAttrs = append(sendSpanAttrs, semconv.MessageUncompressedSizeKey.Int(size))
		}
	}
	p.span.AddEvent("message", trace.WithAttributes(sendSpanAttrs...))
	return err
}
