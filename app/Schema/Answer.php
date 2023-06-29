<?php

namespace App\Schema;

class Answer
{
    public function __construct(
        protected string $answer,
        protected string $type = 'extractive',
        protected ?float $score = null,
        protected ?array $context = null,
        protected ?array $offsetsInDocument = null,
        protected ?array $offsetsInContext = null,
        protected ?array $documentIds = null,
        protected ?array $meta = null
    ) {
    }
}
