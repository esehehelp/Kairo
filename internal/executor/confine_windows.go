//go:build windows

package executor

import (
	"fmt"
	"os"
	"os/exec"
	"runtime"
	"syscall"
	"unsafe"

	"golang.org/x/sys/windows"
)

// On Windows a confined attempt's launcher runs with a copy of the agent's
// token at Low integrity: a process at Low integrity can read what it could
// before but write only what is labelled Low (a mandatory label, no write
// up), which the scratch, the output directory and the cache directory are
// made (labelLow, inherited by what is made in them); its children inherit
// the token. Programs that make named kernel objects at the session's level
// do not start at Low integrity (MSYS and Git Bash tools among them); CUDA,
// Python and native programs do.

// confineCommand labels writable Low and gives cmd a Low integrity token,
// closed by release once the launcher has started.
func confineCommand(cmd *exec.Cmd, writable []string, _ string) (func(), error) {
	for _, w := range writable {
		if err := labelLow(w); err != nil {
			return nil, fmt.Errorf("%w: label %s Low: %v", errConfineHere, w, err)
		}
	}
	token, err := lowIntegrityToken()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errConfineHere, err)
	}
	// with a token, os/exec takes a nil Env as that token's user's default
	// environment (CreateEnvironmentBlock), not this process's: the shim's
	// command would lose the attempt's (KAIRO_*, the declared env, TEMP)
	if cmd.Env == nil {
		cmd.Env = os.Environ()
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Token = syscall.Token(token)
	return func() { token.Close() }, nil
}

// lowIntegrityToken is a primary token, a copy of this process's, at Low
// integrity.
func lowIntegrityToken() (windows.Token, error) {
	var own windows.Token
	if err := windows.OpenProcessToken(windows.CurrentProcess(), windows.TOKEN_DUPLICATE|windows.TOKEN_QUERY|windows.TOKEN_ADJUST_DEFAULT|windows.TOKEN_ASSIGN_PRIMARY, &own); err != nil {
		return 0, fmt.Errorf("open own token: %w", err)
	}
	defer own.Close()
	var low windows.Token
	if err := windows.DuplicateTokenEx(own, windows.MAXIMUM_ALLOWED, nil, windows.SecurityImpersonation, windows.TokenPrimary, &low); err != nil {
		return 0, fmt.Errorf("copy own token: %w", err)
	}
	sid, err := windows.StringToSid("S-1-16-4096") // Mandatory Label\Low Mandatory Level
	if err != nil {
		low.Close()
		return 0, err
	}
	// TOKEN_MANDATORY_LABEL followed by its SID in one buffer: the length
	// passed covers both, so all of it must be ours (issue #9: the kernel
	// probes that many bytes from the label).
	sidLen := windows.GetLengthSid(sid)
	head := unsafe.Sizeof(windows.Tokenmandatorylabel{})
	buf := make([]byte, head+uintptr(sidLen))
	inBuf := (*windows.SID)(unsafe.Pointer(&buf[head]))
	if err := windows.CopySid(sidLen, inBuf, sid); err != nil {
		low.Close()
		return 0, err
	}
	label := (*windows.Tokenmandatorylabel)(unsafe.Pointer(&buf[0]))
	label.Label = windows.SIDAndAttributes{Sid: inBuf, Attributes: windows.SE_GROUP_INTEGRITY}
	err = windows.SetTokenInformation(low, windows.TokenIntegrityLevel, &buf[0], uint32(len(buf)))
	runtime.KeepAlive(buf)
	if err != nil {
		low.Close()
		return 0, fmt.Errorf("lower the token to Low integrity: %w", err)
	}
	return low, nil
}

// labelLow gives dir the Low mandatory label, inherited by the files and
// directories made in it, so a Low integrity process may write there.
func labelLow(dir string) error {
	sd, err := windows.SecurityDescriptorFromString("S:(ML;OICI;NW;;;LW)")
	if err != nil {
		return err
	}
	sacl, _, err := sd.SACL()
	if err != nil {
		return err
	}
	return windows.SetNamedSecurityInfo(dir, windows.SE_FILE_OBJECT, windows.LABEL_SECURITY_INFORMATION, nil, nil, nil, sacl)
}
