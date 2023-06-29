<?php

namespace App\Stores;

use Datashaman\JobChain\HasJobChain;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;

abstract class Store implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use HasJobChain;
    use Queueable;
    use SerializesModels;

    protected string $index;
    protected string $labelIndex;
    protected string $similarity;
    protected array $duplicateDocumentsOptions = [
        'skip',
        'overwrite',
        'fail',
    ];
    protected $idsIterator = null;
}
