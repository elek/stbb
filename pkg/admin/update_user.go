package admin

import (
	"context"

	"github.com/elek/stbb/pkg/db"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"golang.org/x/crypto/bcrypt"
	"storj.io/storj/satellite/console"
)

type UpdateUser struct {
	db.WithDatabase
	Email    string `arg:""`
	Password string
	Status   *int    `name:"status" help:"set user status"`
	Tenant   *string `help:"tenant ID" short:"t"`
}

func (s *UpdateUser) Run() error {
	ctx := context.Background()

	log, err := zap.NewDevelopment()
	if err != nil {
		return errors.WithStack(err)
	}

	satelliteDB, err := s.WithDatabase.GetSatelliteDB(ctx, log)
	if err != nil {
		return errors.WithStack(err)
	}

	user, err := satelliteDB.Console().Users().GetByEmailAndTenant(ctx, s.Email, s.Tenant)
	if err != nil {
		return errors.WithStack(err)
	}

	request := console.UpdateUserRequest{}
	if s.Password != "" {
		raw, err := bcrypt.GenerateFromPassword([]byte(s.Password), 4)
		if err != nil {
			return errors.WithStack(err)
		}
		request.PasswordHash = raw
	}
	if s.Status != nil {
		status := console.UserStatus(*s.Status)
		request.Status = &status
	}

	err = satelliteDB.Console().Users().Update(ctx, user.ID, request)
	if err != nil {
		return errors.WithStack(err)
	}
	return nil
}
