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
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/bufbuild/connect-go"
	pingv1 "github.com/bufbuild/connect-opentelemetry-go/internal/gen/observability/ping/v1"
	"github.com/bufbuild/connect-opentelemetry-go/internal/gen/observability/ping/v1/pingv1connect"
)

func pingHappy(_ context.Context, req *connect.Request[pingv1.PingRequest]) (*connect.Response[pingv1.PingResponse], error) {
	return connect.NewResponse(&pingv1.PingResponse{
		Id:   req.Msg.Id,
		Data: req.Msg.Data,
	}), nil
}

func pingFail(_ context.Context, req *connect.Request[pingv1.PingRequest]) (*connect.Response[pingv1.PingResponse], error) {
	return nil, connect.NewError(connect.CodeDataLoss, errors.New("Oh no"))
}

func sumHappy(
	ctx context.Context,
	stream *connect.ClientStream[pingv1.SumRequest],
) (*connect.Response[pingv1.SumResponse], error) {
	var sum int64
	for stream.Receive() {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		sum += stream.Msg().Number
	}

	err := stream.Err()
	if err != nil && errors.Is(err, io.EOF) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("receive request: %w", err)
	}

	res := connect.NewResponse(&pingv1.SumResponse{
		Sum: sum,
	})

	return res, nil
}

func cumSumHappy(
	ctx context.Context,
	stream *connect.BidiStream[pingv1.CumSumRequest, pingv1.CumSumResponse],
) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		request, err := stream.Receive()
		if err != nil && errors.Is(err, io.EOF) {
			return nil
		} else if err != nil {
			return fmt.Errorf("receive request: %w", err)
		}
		// client
		for i := 0; i < messagesPerRequest; i++ {
			if err := stream.Send(&pingv1.CumSumResponse{Sum: request.Number}); err != nil {
				return fmt.Errorf("send response: %w", err)
			}
		}
	}
}

func cumSumFail(
	ctx context.Context,
	stream *connect.BidiStream[pingv1.CumSumRequest, pingv1.CumSumResponse],
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	request, err := stream.Receive()
	if err != nil && errors.Is(err, io.EOF) {
		return nil
	}
	if err := stream.Send(&pingv1.CumSumResponse{Sum: request.Number}); err != nil {
		return fmt.Errorf("send response: %w", err)
	}
	if err := stream.Send(&pingv1.CumSumResponse{Sum: request.Number}); err != nil {
		return fmt.Errorf("send response: %w", err)
	}
	return connect.NewError(connect.CodeDataLoss, errors.New("Oh no"))
}

func happyPingServer() *pluggablePingServer {
	return &pluggablePingServer{
		ping:   pingHappy,
		sum:    sumHappy,
		cumSum: cumSumHappy,
	}
}

func failPingServer() *pluggablePingServer {
	return &pluggablePingServer{
		ping:   pingFail,
		cumSum: cumSumFail,
	}
}

var _ pingv1connect.PingServiceHandler = &pluggablePingServer{}

type pluggablePingServer struct {
	pingv1connect.UnimplementedPingServiceHandler

	ping   func(context.Context, *connect.Request[pingv1.PingRequest]) (*connect.Response[pingv1.PingResponse], error)
	sum    func(context.Context, *connect.ClientStream[pingv1.SumRequest]) (*connect.Response[pingv1.SumResponse], error)
	cumSum func(context.Context, *connect.BidiStream[pingv1.CumSumRequest, pingv1.CumSumResponse]) error
}

func (p *pluggablePingServer) Ping(
	ctx context.Context,
	request *connect.Request[pingv1.PingRequest],
) (*connect.Response[pingv1.PingResponse], error) {
	return p.ping(ctx, request)
}

func (p *pluggablePingServer) CumSum(
	ctx context.Context,
	stream *connect.BidiStream[pingv1.CumSumRequest, pingv1.CumSumResponse],
) error {
	return p.cumSum(ctx, stream)
}

func (p *pluggablePingServer) Sum(
	ctx context.Context,
	stream *connect.ClientStream[pingv1.SumRequest],
) (*connect.Response[pingv1.SumResponse], error) {
	return p.sum(ctx, stream)
}
