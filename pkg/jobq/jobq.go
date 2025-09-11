package jobq

type Jobq struct {
	Stat    Stat    `cmd:"" help:"Show job queue statistics."`
	Peek    Peek    `cmd:"" help:"Peek into the job queue, showing the next jobs without removing them."`
	Trim    Trim    `cmd:"" help:"Trim the job queue, removing jobs up to a specified placement constraint."`
	Inspect Inspect `cmd:"" help:"Inspect jobs in the job queue, showing detailed information about each job."`
}
