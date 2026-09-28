package eventbus

import (
	"errors"
	"testing"
)

type recoveryUserStub struct {
	IUser
	apply func([]*Conn) bool
}

func (u *recoveryUserStub) BeginConnRecovery(string, uint64) (func([]*Conn) bool, bool) {
	return u.apply, true
}

type noRecoveryUserStub struct{ IUser }

func TestUserRecoveryOptionalCapability(t *testing.T) {
	apply := func([]*Conn) bool { return true }
	supported := newUserPlus(&recoveryUserStub{apply: apply})
	got, needed, err := supported.BeginConnRecovery("u", 2)
	if err != nil || !needed || !got(nil) {
		t.Fatalf("已实现恢复能力应透传回调：needed=%t err=%v", needed, err)
	}
	unsupported := newUserPlus(&noRecoveryUserStub{})
	got, needed, err = unsupported.BeginConnRecovery("u", 2)
	if got != nil || needed || !errors.Is(err, ErrConnRecoveryUnsupported) {
		t.Fatalf("未实现恢复能力应明确报错：needed=%t err=%v", needed, err)
	}
}
