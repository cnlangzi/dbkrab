package dbkrab

import "embed"

//go:embed dashboard/dist/index.html
//go:embed dashboard/dist/static/*
var DashboardFS embed.FS
