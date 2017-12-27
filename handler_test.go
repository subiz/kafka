package kafka_test

import (
	"testing"
	"bitbucket.org/subiz/kafka"
	"context"
	"bitbucket.org/subiz/header/account"
	"bitbucket.org/subiz/gocommon"
	"fmt"
	compb "bitbucket.org/subiz/header/common"
	"bitbucket.org/subiz/header/auth"
)

func Route() {
}

func TestRouter(t *testing.T) {
	var ctx context.Context = context.Background()
	myacc := &account.Account{
		Ctx: &compb.Context{
			Topic: "e128",
			Credential: &auth.Credential{
				ClientId: "client",
			},
		},
		Id: common.Ps("hv"),
	}

	ok := false

	value := common.Protify(myacc)
	es := &kafka.EventStore{}
	r := kafka.NewRouter(es, "", "", kafka.R{
		kafka.Str("e128"): func(acc *account.Account) {
			if acc.GetId() != myacc.GetId() {
				t.Fatalf("should equal, got %s", acc.GetId())
			}

			cred := common.GetCredential(ctx)
			if cred.GetClientId() != "client" {
				t.Fatalf("should equal, got %s", cred.GetClientId())
			}
			ok = true
		},
	})

	r.Handle(&ctx, value, nil, nil)

	if !ok {
		t.Fatal("shoul be call")
	}
}

func BenchmarkRouter(b *testing.B) {
	var ctx context.Context
	r := kafka.NewRouter(nil, "", "", kafka.R{
		kafka.Str("e128"): func(acc *account.Account) {

		},
	})
	myacc := &account.Account{
			Ctx: &compb.Context{
				Topic: "e128",
			},
			Id: common.Ps("hv"),
			Name: common.Ps("very long string, longer then short string"),
			Address: common.Ps("a shorter string"),
		}

	value := common.Protify(myacc)

	for n := 0; n < b.N; n++ {
		r.Handle(&ctx, value, nil, nil)
	}
}

func BenchmarkRouter2(b *testing.B) {
	for n := 0; n < b.N; n++ {
		var ctx context.Context
		myacc := &account.Account{
			Ctx: &compb.Context{
				Topic: "e128",
			},
			Id: common.Ps("hv"),
		}

		value := common.Protify(myacc)
		var m = map[fmt.Stringer]interface{} {
			kafka.Str("e128"): func(acc *account.Account) {

			},
		}

		if 0 == 1 {
			println(value, m, ctx)
		}
	}
}
