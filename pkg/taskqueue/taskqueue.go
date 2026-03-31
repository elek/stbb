package taskqueue

// TaskQueue contains subcommands for inspecting the balancer task queue.
type TaskQueue struct {
	Keys Keys `cmd:"" help:"List all stream keys and their sizes."`
	Stat Stat `cmd:"" help:"Show summary statistics of the task queue."`
	Src  Src  `cmd:"" help:"Show histogram of source nodes in the task queue."`
	Dst  Dst  `cmd:"" help:"Show histogram of destination nodes in the task queue."`
}
