<?php

namespace App\Schema;

use Illuminate\Contracts\Support\Arrayable;
use Illuminate\Support\Str;
use ValueError;

class Document implements Arrayable
{
    public static function make(array $attributes): self
    {
        return new Document(
            id: $attributes['id'],
            content: $attributes['content'],
            contentType: $attributes['contentType'],
            idHashKeys: $attributes['idHashKeys'],
            meta: $attributes['meta'],
            score: $attributes['score'],
            embedding: $attributes['embedding']
        );
    }

    public function __construct(
        public string $content,
        public string $contentType = 'text',
        public ?string $id = null,
        public ?float $score = null,
        public array $meta = [],
        public ?array $embedding = null,
        public ?array $idHashKeys = ['content']
    ) {
        $allowedHashKeys = [
            'content',
            'contentType',
            'score',
            'meta',
            'embedding',
        ];

        if (!is_null($idHashKeys)) {
            foreach ($idHashKeys as $key) {
                if (!(in_array($key, $allowedHashKeys) || Str::startsWith('meta.'))) {
                    $allowed = implode(', ', $allowedHashKeys);
                    throw new ValueError("Hash key '{$key}' cannot be used. Must start with meta. or be one of: {$allowed}");
                }
            }
        }

        $this->idHashKeys = $idHashKeys ?: ['content'];

        $this->id = is_null($id)
            ? $this->getId($idHashKeys)
            : $id;
    }

    public function toArray(): array
    {
        return [
            'id' => $this->id,
            'content' => $this->content,
            'contentType' => $this->contentType,
            'score' => $this->score,
            'meta' => $this->meta,
            'embedding' => $this->embedding,
        ];
    }

    protected function getId(?array $idHashKeys = null): string
    {
        if (is_null($idHashKeys)) {
            return hash('murmur3f', (string) $this->content);
        }

        $context = hash_init('murmur3f');

        foreach ($idHashKeys as $attr) {
            if (preg_match("/^meta\.\(.*\)/", $attr, $matches)) {
                $metaKey = $matches[1];
                if (in_array($metaKey, $this->meta)) {
                    hash_update($context, (string) $this->meta[$metaKey]);
                }
            } else {
                hash_update($context, (string) $this->$attr);
            }
        }

        return hash_final($context);
    }
}
