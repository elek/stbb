package main

import (
	"fmt"
	"github.com/pkg/errors"
	"storj.io/storj/storagenode/iopriority"
	"syscall"
	"time"
)

type IOTest struct {
}

func (i IOTest) Run() error {
	err := iopriority.SetLowIOPriority()
	if err != nil {
		return errors.WithStack(err)
	}
	time.Sleep(1 * time.Hour)
	return nil
}

const (
	ioprioClassShift uint16 = 13
	ioprioClassMask  uint16 = 0x07
	ioprioPrioMask   uint16 = (1 << ioprioClassShift) - 1

	ioprioWhoProcess uint16 = 1
	ioprioClassBE    uint16 = 2
)

// SetLowIOPriority lowers the process I/O priority.
//
// On linux, this sets the I/O priority to "best effort" with a priority class data of 7.
func SetLowIOPriority() error {
	ioprioPrioValue := ioprioPrioClassValue(ioprioClassBE, 7)
	fmt.Printf("%08b\n", ioprioPrioValue)
	fmt.Println("before syscall")
	_, _, err := syscall.Syscall(syscall.SYS_IOPRIO_SET, uintptr(ioprioWhoProcess), 0, uintptr(ioprioPrioValue))
	if err != 0 {
		return err
	}
	return nil
}

// ioprioPrioClassValue returns the class value based on the definition for the IOPRIO_PRIO_VALUE
// macro in Linux's ioprio.h
// See https://github.com/torvalds/linux/blob/61d325dcbc05d8fef88110d35ef7776f3ac3f68b/include/uapi/linux/ioprio.h#L15-L17
func ioprioPrioClassValue(class, data uint16) uint16 {
	fmt.Printf("%08b\n", ((class)&ioprioClassMask)<<ioprioClassShift)
	return (((class) & ioprioClassMask) << ioprioClassShift) | ((data) & ioprioPrioMask)
}
