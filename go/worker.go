/*
Create a worker pool for field scoring.

References:
- https://gobyexample.com/worker-pools
- https://livebook.manning.com/book/go-in-practice/chapter-3/16
- https://github.com/tmrts/go-patterns/blob/master/concurrency/parallelism.go
- http://marcio.io/2015/07/handling-1-million-requests-per-minute-with-golang/
*/
package main

// Job represents the job to be run
type Job struct {
	Doc Doc
}

// JobQueue carries inputs to the dispatcher
var JobQueue chan Job

// ResultQueue carries outputs back to the main thread
var ResultQueue chan *DocScores

// Worker represents the worker that executes the job
type Worker struct {
	WorkerPool chan chan Job
	JobChannel chan Job
	Scorer     *Scorer
	Vectors    *Vectors
	quit       chan bool
}

func NewWorker(workerPool *chan chan Job, scorer *Scorer, vectors *Vectors) Worker {
	return Worker{
		WorkerPool: *workerPool,
		JobChannel: make(chan Job),
		Scorer:     scorer,
		Vectors:    vectors,
		quit:       make(chan bool),
	}
}

// Start method starts the run loop for the worker, listening for a quit channel in
// case we need to stop it
func (w Worker) Start() {
	go func() {
		for {
			// register the current worker into the worker queue.
			w.WorkerPool <- w.JobChannel

			select {
			case job := <-w.JobChannel:
				// we have received a work request.
				embedding := w.Vectors.Embed(&job.Doc)
				scores := w.Scorer.Score(&embedding)
				ResultQueue <- scores

			case <-w.quit:
				// we have received a signal to stop
				return
			}
		}
	}()
}

// Stop signals the worker to stop listening for work requests.
func (w Worker) Stop() {
	go func() {
		w.quit <- true
	}()
}

type Dispatcher struct {
	// A pool of workers channels that are registered with the dispatcher
	WorkerPool chan chan Job
	MaxWorkers int
	Scorer     *Scorer
	Vectors    *Vectors
	Workers    []*Worker
}

func NewDispatcher(maxWorkers int, includeAll bool) *Dispatcher {
	pool := make(chan chan Job, maxWorkers)
	vectors := NewVectors()
	scorer := NewScorer(includeAll)
	return &Dispatcher{
		WorkerPool: pool,
		MaxWorkers: maxWorkers,
		Scorer:     scorer,
		Vectors:    vectors,
	}
}

func (d *Dispatcher) Run() {
	// starting n number of workers
	for i := 0; i < d.MaxWorkers; i++ {
		worker := NewWorker(&d.WorkerPool, d.Scorer, d.Vectors)
		worker.Start()
		d.Workers = append(d.Workers, &worker)
	}

	go d.dispatch()
}

func (d *Dispatcher) Stop() {
	// stopping n number of workers
	for _, w := range d.Workers {
		w.Stop()
	}
	close(JobQueue)
}

func (d *Dispatcher) dispatch() {
	for {
		select {
		case job := <-JobQueue:
			//a job request has been received
			// try to obtain a worker job channel that is available.
			// this will block until a worker is idle
			jobChannel := <-d.WorkerPool
			// dispatch the job to the worker job channel
			jobChannel <- job
		}
	}
}
