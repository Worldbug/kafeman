package main

import (
	"context"
	"kafeman/internal/admin"
	"kafeman/internal/config"
)

func main() {
	ctx := context.Background()
	cfg, _ := config.LoadConfig("")

	adm := admin.NewAdmin(cfg)

	adm.DescribeTopic(ctx, "processing-state-events")

}
