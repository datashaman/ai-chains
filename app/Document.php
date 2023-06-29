<?php

namespace App;

use Exception;
use Illuminate\Support\Str;

class Document
{
    public function __construct(
        public string $content,
        public string $contentType = 'text',
        public ?string $id = null,
    ) {
        $this->id = $id ?: Str::uuid();
    }
}
