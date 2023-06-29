<?php

namespace App\Jobs;

use Datashaman\JobChain\HasJobChain;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;

abstract class Job implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use HasJobChain;
    use Queueable;
    use SerializesModels;
}