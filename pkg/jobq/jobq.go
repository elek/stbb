package jobq

type Jobq struct {
	Stat Stat `cmd:"" help:"Show job queue statistics."`
	Peek Peek `cmd:"" help:"Peek into the job queue, showing the next jobs without removing them."`
}
