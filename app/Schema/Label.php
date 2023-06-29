<?php

namespace App\Schema;

use Illuminate\Support\Str;

class Label
{
    public function __construct(
        protected string $query,
        protected Document $document,
        protected bool $isCorrectAnswer,
        protected bool $isCorrectDocument,
        protected string $origin,
        protected ?Answer $answer = null,
        protected ?string $id = null,
        protected ?string $pipelineId = null,
        protected ?string $createdAt = null,
        protected ?string $updatedAt = null,
        protected ?array $meta = null,
        protected ?array $filters = null
    ) {
        $this->id = $id ?: Str::uuid();
        $this->createdAt = $createdAt ?: date('Y-m-d H:i:s');
        $this->meta = $meta ?: [];
    }
}
