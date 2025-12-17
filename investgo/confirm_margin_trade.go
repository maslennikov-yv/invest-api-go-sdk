package investgo

import (
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/jhump/protoreflect/desc/protoparse"
	pb "github.com/maslennikov-yv/invest-api-go-sdk/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
)

const (
	stopOrdersPostMethod = "/tinkoff.public.invest.api.contract.v1.StopOrdersService/PostStopOrder"
	ordersPostMethod     = "/tinkoff.public.invest.api.contract.v1.OrdersService/PostOrder"
)

type confirmProtoCache struct {
	once sync.Once
	err  error

	postStopOrderMD protoreflect.MessageDescriptor
	postOrderMD     protoreflect.MessageDescriptor
	quotationMD     protoreflect.MessageDescriptor
}

var confirmCache confirmProtoCache

// NOTE: We MUST NOT read .proto files from disk at runtime. In production deployments we ship
// only the compiled binary (no module cache / source tree), so filesystem-based parsing breaks.
// Keep these minimal proto definitions embedded as strings and parse via a custom accessor.
var confirmProtoFiles = map[string]string{
	"google/protobuf/timestamp.proto": `syntax = "proto3";

package google.protobuf;

option go_package = "google.golang.org/protobuf/types/known/timestamppb";

message Timestamp {
  int64 seconds = 1;
  int32 nanos = 2;
}
`,
	"common.proto": `syntax = "proto3";

package tinkoff.public.invest.api.contract.v1;

option go_package = "./;investapi";

import "google/protobuf/timestamp.proto";

message Quotation {
  int64 units = 1;
  int32 nano = 2;
}
`,
	"orders.proto": `syntax = "proto3";

package tinkoff.public.invest.api.contract.v1;

option go_package = "./;investapi";

import "common.proto";
import "google/protobuf/timestamp.proto";

service OrdersService {
  rpc PostOrder(PostOrderRequest) returns (PostOrderResponse);
}

enum OrderDirection {
  ORDER_DIRECTION_UNSPECIFIED = 0;
  ORDER_DIRECTION_BUY = 1;
  ORDER_DIRECTION_SELL = 2;
}

enum OrderType {
  ORDER_TYPE_UNSPECIFIED = 0;
  ORDER_TYPE_LIMIT = 1;
  ORDER_TYPE_MARKET = 2;
  ORDER_TYPE_BESTPRICE = 3;
}

message PostOrderRequest {
  string figi = 1 [ deprecated = true ];
  int64 quantity = 2;
  Quotation price = 3;
  OrderDirection direction = 4;
  string account_id = 5;
  OrderType order_type = 6;
  string order_id = 7;
  string instrument_id = 8;
  bool confirm_margin_trade = 9;
}

message PostOrderResponse {
  string order_id = 1;
}
`,
	"stoporders.proto": `syntax = "proto3";

package tinkoff.public.invest.api.contract.v1;

option go_package = "./;investapi";

import "google/protobuf/timestamp.proto";
import "common.proto";

service StopOrdersService {
  rpc PostStopOrder(PostStopOrderRequest) returns (PostStopOrderResponse);
}

enum StopOrderDirection {
  STOP_ORDER_DIRECTION_UNSPECIFIED = 0;
  STOP_ORDER_DIRECTION_BUY = 1;
  STOP_ORDER_DIRECTION_SELL = 2;
}

enum StopOrderExpirationType {
  STOP_ORDER_EXPIRATION_TYPE_UNSPECIFIED = 0;
  STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_CANCEL = 1;
  STOP_ORDER_EXPIRATION_TYPE_GOOD_TILL_DATE = 2;
}

enum StopOrderType {
  STOP_ORDER_TYPE_UNSPECIFIED = 0;
  STOP_ORDER_TYPE_TAKE_PROFIT = 1;
  STOP_ORDER_TYPE_STOP_LOSS = 2;
  STOP_ORDER_TYPE_STOP_LIMIT = 3;
}

message PostStopOrderRequest {
  string figi = 1 [ deprecated = true ];
  int64 quantity = 2;
  Quotation price = 3;
  Quotation stop_price = 4;
  StopOrderDirection direction = 5;
  string account_id = 6;
  StopOrderExpirationType expiration_type = 7;
  StopOrderType stop_order_type = 8;
  google.protobuf.Timestamp expire_date = 9;
  string instrument_id = 10;
  bool confirm_margin_trade = 11;
}

message PostStopOrderResponse {
  string stop_order_id = 1;
}
`,
}

func loadConfirmDescriptors() error {
	confirmCache.once.Do(func() {
		p := protoparse.Parser{
			Accessor: func(filename string) (io.ReadCloser, error) {
				if s, ok := confirmProtoFiles[filename]; ok {
					return io.NopCloser(strings.NewReader(s)), nil
				}
				return nil, fmt.Errorf("proto file not found: %s", filename)
			},
		}

		fds, err := p.ParseFiles(
			"google/protobuf/timestamp.proto",
			"common.proto",
			"stoporders.proto",
			"orders.proto",
		)
		if err != nil {
			confirmCache.err = fmt.Errorf("parse proto files for confirm_margin_trade: %w", err)
			return
		}

		set := &descriptorpb.FileDescriptorSet{}
		for _, fd := range fds {
			set.File = append(set.File, fd.AsFileDescriptorProto())
		}

		files, err := protodesc.NewFiles(set)
		if err != nil {
			confirmCache.err = fmt.Errorf("build descriptors for confirm_margin_trade: %w", err)
			return
		}

		stopFD, err := files.FindFileByPath("stoporders.proto")
		if err != nil {
			confirmCache.err = fmt.Errorf("find stoporders.proto descriptor: %w", err)
			return
		}
		ordersFD, err := files.FindFileByPath("orders.proto")
		if err != nil {
			confirmCache.err = fmt.Errorf("find orders.proto descriptor: %w", err)
			return
		}
		commonFD, err := files.FindFileByPath("common.proto")
		if err != nil {
			confirmCache.err = fmt.Errorf("find common.proto descriptor: %w", err)
			return
		}

		confirmCache.postStopOrderMD = stopFD.Messages().ByName("PostStopOrderRequest")
		if confirmCache.postStopOrderMD == nil {
			confirmCache.err = fmt.Errorf("PostStopOrderRequest not found in stoporders.proto descriptor")
			return
		}
		confirmCache.postOrderMD = ordersFD.Messages().ByName("PostOrderRequest")
		if confirmCache.postOrderMD == nil {
			confirmCache.err = fmt.Errorf("PostOrderRequest not found in orders.proto descriptor")
			return
		}
		confirmCache.quotationMD = commonFD.Messages().ByName("Quotation")
		if confirmCache.quotationMD == nil {
			confirmCache.err = fmt.Errorf("Quotation not found in common.proto descriptor")
			return
		}
	})

	return confirmCache.err
}

func dynQuotation(q *pb.Quotation) (protoreflect.Value, error) {
	if q == nil {
		return protoreflect.Value{}, nil
	}
	if err := loadConfirmDescriptors(); err != nil {
		return protoreflect.Value{}, err
	}
	m := dynamicpb.NewMessage(confirmCache.quotationMD)
	fUnits := confirmCache.quotationMD.Fields().ByName("units")
	fNano := confirmCache.quotationMD.Fields().ByName("nano")
	if fUnits == nil || fNano == nil {
		return protoreflect.Value{}, fmt.Errorf("Quotation fields not found in descriptor")
	}
	m.Set(fUnits, protoreflect.ValueOfInt64(q.GetUnits()))
	m.Set(fNano, protoreflect.ValueOfInt32(q.GetNano()))
	return protoreflect.ValueOfMessage(m), nil
}

func buildPostOrderDynamic(req *PostOrderRequest) (*dynamicpb.Message, error) {
	if err := loadConfirmDescriptors(); err != nil {
		return nil, err
	}
	in := dynamicpb.NewMessage(confirmCache.postOrderMD)
	setStr(in, "instrument_id", req.InstrumentId)
	setI64(in, "quantity", req.Quantity)
	setEnum(in, "direction", int32(req.Direction))
	setStr(in, "account_id", req.AccountId)
	setEnum(in, "order_type", int32(req.OrderType))
	setStr(in, "order_id", req.OrderId)

	if req.Price != nil {
		v, err := dynQuotation(req.Price)
		if err != nil {
			return nil, err
		}
		setMsg(in, "price", v.Message())
	}

	setBool(in, "confirm_margin_trade", true)
	return in, nil
}

func buildPostStopOrderDynamic(req *PostStopOrderRequest) (*dynamicpb.Message, error) {
	if err := loadConfirmDescriptors(); err != nil {
		return nil, err
	}
	in := dynamicpb.NewMessage(confirmCache.postStopOrderMD)
	setStr(in, "instrument_id", req.InstrumentId)
	setI64(in, "quantity", req.Quantity)
	setEnum(in, "direction", int32(req.Direction))
	setStr(in, "account_id", req.AccountId)
	setEnum(in, "expiration_type", int32(req.ExpirationType))
	setEnum(in, "stop_order_type", int32(req.StopOrderType))

	if req.StopPrice != nil {
		v, err := dynQuotation(req.StopPrice)
		if err != nil {
			return nil, err
		}
		setMsg(in, "stop_price", v.Message())
	}
	if req.Price != nil {
		v, err := dynQuotation(req.Price)
		if err != nil {
			return nil, err
		}
		setMsg(in, "price", v.Message())
	}

	setBool(in, "confirm_margin_trade", true)
	return in, nil
}

func setStr(m *dynamicpb.Message, name, v string) {
	if v == "" {
		return
	}
	if f := m.Descriptor().Fields().ByName(protoreflect.Name(name)); f != nil {
		m.Set(f, protoreflect.ValueOfString(v))
	}
}

func setI64(m *dynamicpb.Message, name string, v int64) {
	if v == 0 {
		return
	}
	if f := m.Descriptor().Fields().ByName(protoreflect.Name(name)); f != nil {
		m.Set(f, protoreflect.ValueOfInt64(v))
	}
}

func setBool(m *dynamicpb.Message, name string, v bool) {
	if f := m.Descriptor().Fields().ByName(protoreflect.Name(name)); f != nil {
		m.Set(f, protoreflect.ValueOfBool(v))
	}
}

func setEnum(m *dynamicpb.Message, name string, v int32) {
	if f := m.Descriptor().Fields().ByName(protoreflect.Name(name)); f != nil {
		m.Set(f, protoreflect.ValueOfEnum(protoreflect.EnumNumber(v)))
	}
}

func setMsg(m *dynamicpb.Message, name string, mv protoreflect.Message) {
	if f := m.Descriptor().Fields().ByName(protoreflect.Name(name)); f != nil && mv.IsValid() {
		m.Set(f, protoreflect.ValueOfMessage(mv))
	}
}
