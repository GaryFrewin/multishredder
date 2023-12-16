class Config:
    def __init__(self, args):
        self.chunk_size = args.chunksize
        self.num_workers = args.workers
        self.num_writers = args.writers
        self.batch_size = args.batchsize
