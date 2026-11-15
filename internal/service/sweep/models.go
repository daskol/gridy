package sweep

import (
	"github.com/daskol/gridy/internal/service/storageapi"
	sweep_api "github.com/daskol/gridy/pkg/api/sweep/v1"
	"github.com/daskol/gridy/pkg/storage"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func marshalSweep(sweep storage.Sweep) (*sweep_api.Sweep, error) {
	space, err := storageapi.Object(sweep.Space)
	if err != nil {
		return nil, err
	}

	state := sweep_api.SweepState_SWEEP_STATE_UNSPECIFIED
	switch sweep.State {
	case storage.SweepPending:
		state = sweep_api.SweepState_SWEEP_STATE_PENDING
	case storage.SweepRunning:
		state = sweep_api.SweepState_SWEEP_STATE_RUNNING
	case storage.SweepCompleted:
		state = sweep_api.SweepState_SWEEP_STATE_COMPLETED
	case storage.SweepFailed:
		state = sweep_api.SweepState_SWEEP_STATE_FAILED
	default:
		return nil, status.Error(codes.Internal, "invalid stored sweep state")
	}

	return &sweep_api.Sweep{
		SweepId:   sweep.ID,
		Code:      sweep.Code,
		State:     state,
		Space:     &sweep_api.SearchSpace{Parameters: space},
		CreatedAt: timestamppb.New(sweep.CreatedAt),
		UpdatedAt: timestamppb.New(sweep.UpdatedAt),
	}, nil
}

func marshalSweeps(sweeps []storage.Sweep) ([]*sweep_api.Sweep, error) {
	items := make([]*sweep_api.Sweep, 0, len(sweeps))
	for _, sweep := range sweeps {
		item, err := marshalSweep(sweep)
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, nil
}

func marshalTrial(trial storage.Trial) (*sweep_api.Trial, error) {
	params, err := storageapi.Object(trial.Params)
	if err != nil {
		return nil, err
	}
	result, err := storageapi.Object(trial.Result)
	if err != nil {
		return nil, err
	}

	state := sweep_api.TrialState_TRIAL_STATE_UNSPECIFIED
	switch trial.State {
	case storage.TrialPending:
		state = sweep_api.TrialState_TRIAL_STATE_PENDING
	case storage.TrialRunning:
		state = sweep_api.TrialState_TRIAL_STATE_RUNNING
	case storage.TrialCompleted:
		state = sweep_api.TrialState_TRIAL_STATE_COMPLETED
	case storage.TrialFailed:
		state = sweep_api.TrialState_TRIAL_STATE_FAILED
	default:
		return nil, status.Error(codes.Internal, "invalid stored trial state")
	}

	return &sweep_api.Trial{
		SweepId:   trial.SweepID,
		TrialId:   trial.TrialID,
		AttemptId: trial.AttemptID,
		State:     state,
		Params:    params,
		Result:    result,
		Error:     trial.Error,
		CreatedAt: timestamppb.New(trial.CreatedAt),
		UpdatedAt: timestamppb.New(trial.UpdatedAt),
	}, nil
}
