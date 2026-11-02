package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/firecracker-microvm/firecracker-go-sdk"
	"github.com/firecracker-microvm/firecracker-go-sdk/vsock"
	agentv1 "github.com/hostinger/fireactions/proto/agent/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Machine holds metadata about a Firecracker machine and its associated resources.
type Machine struct {
	*firecracker.Machine

	Name         string
	RunnerID     int64
	Pool         string
	Organization string
	ProcessID    int
	CreatedAt    time.Time
	MemoryMib    int64
	VCPUCount    int64
	WorkflowName string
	JobName      string
	Reservation  *CapacityReservation

	vsockCID      uint32
	vsockPath     string
	leaseCancel   func(context.Context) error // containerd lease cancel function
	vmmCtx        context.Context
	vmmCancel     context.CancelFunc
	waitFunc      func(context.Context) error
	runnerStateFn func(context.Context) (string, error)
	stopFunc      func() error
	stopping      bool
}

func (m *Machine) ConnectToGuestAgent(ctx context.Context) (*grpc.ClientConn, agentv1.AgentServiceClient, error) {
	dialer := func(ctx context.Context, addr string) (net.Conn, error) {
		return vsock.DialContext(ctx, m.vsockPath, 9001)
	}

	// Create gRPC client with VSOCK transport
	// Use "passthrough:" resolver to bypass name resolution and pass directly to dialer
	conn, err := grpc.NewClient(
		"passthrough:vsock",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(dialer),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("grpc dial: %w", err)
	}

	client := agentv1.NewAgentServiceClient(conn)
	return conn, client, nil
}

func (m *Machine) GetAddr() string {
	addr := ""
	if m.Machine != nil && len(m.Cfg.NetworkInterfaces) > 0 {
		addr = m.Cfg.NetworkInterfaces[0].StaticConfiguration.IPConfiguration.IPAddr.IP.String()
	}

	return addr
}

func (m *Machine) WaitForExit(ctx context.Context) error {
	if m.waitFunc != nil {
		return m.waitFunc(ctx)
	}

	if m.Machine == nil {
		return nil
	}

	return m.Wait(ctx)
}

func (m *Machine) GetRunnerState(ctx context.Context) (string, error) {
	if m.runnerStateFn != nil {
		return m.runnerStateFn(ctx)
	}

	conn, client, err := m.ConnectToGuestAgent(ctx)
	if err != nil {
		return "", fmt.Errorf("connect to agent: %w", err)
	}
	defer conn.Close()

	resp, err := client.GetRunnerState(ctx, &agentv1.GetRunnerStateRequest{})
	if err != nil {
		return "", fmt.Errorf("agent GetRunnerState: %w", err)
	}

	return resp.GetState(), nil
}

func (m *Machine) IsIdleForScaleDown(ctx context.Context) (bool, string, error) {
	state, err := m.GetRunnerState(ctx)
	if err != nil {
		return false, "", err
	}

	switch state {
	case "Idle", "Completed", "Exited", "Error":
		return true, state, nil
	default:
		return false, state, nil
	}
}

func (m *Machine) Stop() error {
	if m.stopFunc != nil {
		return m.stopFunc()
	}

	if m.Machine == nil {
		return nil
	}

	return m.StopVMM()
}
