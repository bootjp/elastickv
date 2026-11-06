package raftengine

type NativeConfigurationMember struct {
	ID      uint64
	Address string
}

// NativeConfiguration is the engine's actual pre-entry applied configuration,
// copied on its serial native/apply loop. It is not a wire assertion or a
// snapshot data index. Consumers must clear it after the corresponding Apply.
type NativeConfiguration struct {
	Index                                          uint64
	Voters, VotersOutgoing, Learners, LearnersNext []uint64
	AutoLeave                                      bool
	Members                                        []NativeConfigurationMember
}

type ApplyNativeConfigurationAware interface {
	SetApplyNativeConfiguration(NativeConfiguration)
	ClearApplyNativeConfiguration()
}

// ApplyAdmissionChangeAware checks the current durable FSM projection outside
// the admission lock, on the serial apply loop. It never grants permission or
// trusts policy hints in a received command. Operational errors abort apply.
type ApplyAdmissionChangeAware interface {
	ApplyChangesAdmission(payload []byte) (bool, error)
}
