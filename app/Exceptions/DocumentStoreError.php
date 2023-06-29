<?php

namespace App\Exceptions;

use Exception;

class DocumentStoreError extends JobChainException
{
    public function report(): void
    {
        dd('here');
    }
}
