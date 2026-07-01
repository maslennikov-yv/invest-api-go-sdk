package investgo

import (
	"context"

	pb "github.com/maslennikov-yv/invest-api-go-sdk/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type StopOrdersServiceClient struct {
	conn     *grpc.ClientConn
	config   Config
	logger   Logger
	ctx      context.Context
	pbClient pb.StopOrdersServiceClient
}

// PostStopOrder - Метод выставления стоп-заявки
func (s *StopOrdersServiceClient) PostStopOrder(req *PostStopOrderRequest) (*PostStopOrderResponse, error) {
	var header, trailer metadata.MD
	var (
		resp *pb.PostStopOrderResponse
		err  error
	)
	if req != nil && req.ConfirmMarginTrade {
		if s.logger != nil {
			s.logger.Infof("PostStopOrder: confirm_margin_trade=true (dynamic invoke) instrument_id=%s account_id=%s direction=%v qty=%d stop_order_type=%v",
				req.InstrumentId, req.AccountId, req.Direction, req.Quantity, req.StopOrderType)
		}
		in, buildErr := buildPostStopOrderDynamic(req)
		if buildErr != nil {
			return &PostStopOrderResponse{PostStopOrderResponse: nil, Header: header}, buildErr
		}
		// Extra sanity log: ensure the dynamic message actually has confirm_margin_trade=true set.
		if s.logger != nil {
			if f := in.Descriptor().Fields().ByName("confirm_margin_trade"); f != nil {
				s.logger.Infof("PostStopOrder: dynamic field confirm_margin_trade=%v message=%s", in.Get(f).Bool(), in.Descriptor().FullName())
			} else {
				s.logger.Infof("PostStopOrder: dynamic field confirm_margin_trade NOT FOUND in message=%s", in.Descriptor().FullName())
			}
		}
		resp = &pb.PostStopOrderResponse{}
		err = s.conn.Invoke(s.ctx, stopOrdersPostMethod, in, resp, grpc.Header(&header), grpc.Trailer(&trailer))
	} else {
		resp, err = s.pbClient.PostStopOrder(s.ctx, &pb.PostStopOrderRequest{
			Quantity:       req.Quantity,
			Price:          req.Price,
			StopPrice:      req.StopPrice,
			Direction:      req.Direction,
			AccountId:      req.AccountId,
			ExpirationType: req.ExpirationType,
			StopOrderType:  req.StopOrderType,
			ExpireDate:     TimeToTimestamp(req.ExpireDate),
			InstrumentId:   req.InstrumentId,
		}, grpc.Header(&header), grpc.Trailer(&trailer))
	}
	if err != nil {
		header = trailer
	}
	return &PostStopOrderResponse{
		PostStopOrderResponse: resp,
		Header:                header,
	}, err
}

// GetStopOrders - Метод получения списка активных стоп заявок по счёту
func (s *StopOrdersServiceClient) GetStopOrders(accountId string) (*GetStopOrdersResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.GetStopOrders(s.ctx, &pb.GetStopOrdersRequest{
		AccountId: accountId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &GetStopOrdersResponse{
		GetStopOrdersResponse: resp,
		Header:                header,
	}, err
}

// CancelStopOrder - Метод отмены стоп-заявки
func (s *StopOrdersServiceClient) CancelStopOrder(accountId, stopOrderId string) (*CancelStopOrderResponse, error) {
	var header, trailer metadata.MD
	resp, err := s.pbClient.CancelStopOrder(s.ctx, &pb.CancelStopOrderRequest{
		AccountId:   accountId,
		StopOrderId: stopOrderId,
	}, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		header = trailer
	}
	return &CancelStopOrderResponse{
		CancelStopOrderResponse: resp,
		Header:                  header,
	}, err
}
