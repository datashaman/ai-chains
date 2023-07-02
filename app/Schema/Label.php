<?php

namespace App\Schema;

use Illuminate\Support\Arr;
use Illuminate\Support\Facades\App;
use Illuminate\Support\Str;

class Label
{
    public static function make(array $attributes): self
    {
        $answer = Arr::get($attributes, 'answer');

        if (is_array($answer)) {
            $attributes['answer'] = Answer::make($attributes['answer']);
        }

        $document = Arr::get($attributes, 'document');

        if (is_array($document)) {
            $attributes['document'] = Document::make($attributes['document']);
        }

        return App::make(static::class, $attributes);
    }

    public function __construct(
        public string $query,
        public Document $document,
        public bool $isCorrectAnswer,
        public bool $isCorrectDocument,
        public string $origin,
        public ?Answer $answer = null,
        public ?string $id = null,
        public ?string $pipelineId = null,
        public ?string $createdAt = null,
        public ?string $updatedAt = null,
        public ?array $meta = null,
        public ?array $filters = null
    ) {
        $this->id = $id ?: Str::uuid();
        $this->createdAt = $createdAt ?: date('Y-m-d H:i:s');
        $this->meta = $meta ?: [];
    }

    public function toArray()
    {
        return (array) $this;
    }
}
