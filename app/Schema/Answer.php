<?php

namespace App\Schema;

use Illuminate\Support\Facades\App;

class Answer
{
    public static function make(array $attributes)
    {
        return App::make(static::class, $attributes);
    }

    public function __construct(
        public string $answer,
        public string $type = 'extractive',
        public ?float $score = null,
        public ?array $context = null,
        public ?array $offsetsInDocument = null,
        public ?array $offsetsInContext = null,
        public ?array $documentIds = null,
        public ?array $meta = null
    ) {
    }
}
