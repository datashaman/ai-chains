<?php

namespace App\Jobs;

class PreProcessorJob extends Job
{
    public function __construct(
        protected array $documents
    ) {
    }

    public function handle()
    {
        $this->done($this->documents);
    }
}
