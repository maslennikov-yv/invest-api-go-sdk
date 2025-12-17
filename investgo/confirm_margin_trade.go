package investgo

import (
	"fmt"
	"path/filepath"
	"runtime"
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

func loadConfirmDescriptors() error {
	confirmCache.once.Do(func() {
		_, thisFile, _, _ := runtime.Caller(0)
		// thisFile = .../investgo/confirm_margin_trade.go
		confirmProtoDir := filepath.Clean(filepath.Join(filepath.Dir(thisFile), "..", "proto_confirm"))

		p := protoparse.Parser{
			ImportPaths: []string{confirmProtoDir},
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
