<?php

namespace App\Stores;

use App\Schema\Document;
use Generator;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Log;
use MathPHP\Functions\Special;

abstract class SearchEngineStore extends Store
{
    public function __construct(
        protected array $documents,
        protected $client,
        protected string $index = 'document',
        protected string $labelIndex = 'label',
        protected array $searchFields = ['content'],
        protected string $contentField = 'content',
        protected string $nameField = 'name',
        protected string $embeddingField = 'embedding',
        protected int $embeddingDim = 768,
        protected ?array $customMapping = null,
        protected bool $recreateIndex = false,
        protected bool $createIndex = false,
        protected string $refreshType = 'wait_for',
        protected string $similarity = 'dot_product',
        protected bool $returnEmbedding = false,
        protected string $duplicateDocuments = 'overwrite',
        protected string $scroll = '1d',
        protected ?array $synonyms = null,
        protected string $synonymType = 'synonym',
        protected int $batchSize = 10_000
    ) {
        if (!in_array($similarity, ['cosine', 'dot_product', 'l2'])) {
            throw new DocumentStoreError(
                "Invalid value {$similarity} for similarity, choose between 'cosine', 'l2' and 'dot_product'"
            );
        }

        $this->initIndices(
            $index,
            $labelIndex,
            $createIndex,
            $recreateIndex
        );
    }

    public function getAllDocuments(
        ?string $index = null,
        ?FilterType $filters = null,
        ?bool $returnEmbedding = null,
        int $batchSize = 10_000,
        ?array $headers = null
    ): array {
        return iterator_to_array(
            $this->getAllDocumentsGenerator(
                index: $index,
                filters: $filters,
                returnEmbedding: $returnEmbedding,
                batchSize: $batchSize,
                headers: $headers
            )
        );
    }

    public function writeDocuments(
        array $documents,
        ?string $index = null,
        ?int $batchSize = null,
        ?string $duplicateDocuments = null,
        ?array $headers = null
    ) {
        $index = $index ?: $this->index;

        if ($index && !$this->indexExists($index, headers: $headers)) {
            $this->createDocumentIndex($index, headers: $headers);
        }

        $batchSize = $batchSize ?: $this->batchSize;
        $duplicateDocuments = $duplicateDocuments ?: $this->duplicateDocuments;

        if (!in_array($duplicateDocuments, $this->duplicateDocumentsOptions)) {
            $options = implode(', ', $this->duplicateDocuments);
            throw new DocumentStoreError("duplicateDocuments must be one of: {$options}");
        }

        $documentsToIndex = [];

        foreach ($documents as $document) {
            $doc = $document->toArray();

            Arr::forget($doc, 'score');
            $doc = array_filter($doc);

            if (Arr::has($doc, 'meta')) {
                foreach ($doc['meta'] as $k => $v) {
                    $doc[$k] = $v;
                }
                Arr::forget($doc, 'meta');
            }

            $documentsToIndex[] = $doc;

            if (count($documentsToIndex) % $batchSize == 0) {
                $this->bulk($documentsToIndex, refresh: $this->refreshType, headers: $headers);
                $documentsToIndex = [];
            }
        }

        if ($documentsToIndex) {
            $this->bulk($documentsToIndex, refresh: $this->refreshType, headers: $headers);
        }
    }

    protected function bulk(
        array $documents,
        ?array $headers = null,
        string $refresh = 'wait_for',
        int $timeout = 1,
        int $remainingTries = 10
    ) {
        try {
            $this->doBulk(
                $documents,
                refresh: $this->refreshType,
                headers: $headers
            );
        } catch (Throwable $exception) {
            dd($exception);
        }
    }

    protected function initIndices(
        string $index,
        string $labelIndex,
        bool $createIndex,
        bool $recreateIndex
    ) {
        if ($recreateIndex) {
            $this->deleteIndex($index);
            $this->deleteIndex($labelIndex);
        }

        if (!$this->indexExists($index) && ($createIndex || $recreateIndex)) {
            $this->createDocumentIndex($index);
        }

        if ($this->customMapping) {
            Log::warning('Cannot validate index for custom mappings. Skipping index validation.');
        } else {
            $this->validateAndAdjustDocumentIndex($index);
        }

        if (!$this->indexExists($labelIndex) && ($createIndex || $recreateIndex)) {
            $this->createLabelIndex($labelIndex);
        }
    }

    protected function deleteIndex(string $index)
    {
        if ($index === $this->index) {
            $className = static::class;

            Log::warning(
                "Deletion of default index '{$index}' detected. "
                . "If you plan to use this index again, please reinstantiate '{$className}' in order to avoid side-effects."
            );
        }

        $this->_deleteIndex($index);
    }

    protected function _deleteIndex(string $index)
    {
        if ($this->indexExists($index)) {
            $this->client->indices()->delete([
                'index' => $index,
                'client' => [
                    'ignore' => [400, 404],
                ],
            ]);
            Log::info("Index '{$index}' deletetd.");
        }
    }

    protected function indexExists(
        string $indexName,
        ?array $headers = null
    ): bool {
        if ($this->client->indices()->existsAlias(['name' => $indexName])) {
            Log::debug("Index name {$indexName} is an alias.");
        }

        return $this->client->indices()->exists([
            'index' => $indexName,
            'client' => [
                'headers' => $headers,
            ],
        ]);
    }

    abstract protected function createDocumentIndex(string $indexName, ?array $headers = null);
    abstract protected function createLabelIndex(string $indexName, ?array $headers = null);

    protected function getAllDocumentsGenerator(
        ?string $index = null,
        ?FilterType $filters = null,
        ?bool $returnEmbedding = null,
        int $batchSize = 10_000,
        ?array $headers = null
    ): Generator {
        $index = $index ?: $this->index;
        $returnEmbedding = $returnEmbedding ?: $this->returnEmbedding;

        $excludes = [];
        if (!$returnEmbedding && $this->embeddingField) {
            $excludes = [$this->embeddingField];
        }

        $result = $this->getAllDocumentsInIndex(
            index: $index,
            filters: $filters,
            batchSize: $batchSize,
            headers: $headers,
            excludes: $excludes
        );

        foreach ($result as $hit) {
            yield $this->convertEsHitToDocument($hit);
        }
    }

    protected function getAllDocumentsInIndex(
        string $index,
        ?array $filters = null,
        int $batchSize = 10_000,
        bool $onlyDocumentsWithoutEmbedding = false,
        ?array $headers = null,
        ?array $excludes = null
    ): Generator {
        $query = [];

        if ($filters) {
            $query['bool'] = [];
            $query['bool']['filter'] = LogicalFilterClause::parse($filters)->convertToElasticsearch();
        }

        if ($onlyDocumentsWithoutEmbedding) {
            $query['bool'] = $query['bool'] ?? [];
            $query["bool"]["must_not"] = [
                [
                    "exists" => [
                        "field" => $this->embeddingField,
                    ],
                ],
            ];
        }

        if (!$query) {
            $query = [
                'match_all' => (object) [],
            ];
        }

        $body['query'] = $query;

        if ($excludes) {
            $body["_source"] = [
                "excludes" => $excludes,
            ];
        }

        $searchParams = [
            'body' => $body,
            'index' => $index,
            'size' => $batchSize,
            'scroll' => $this->scroll,
            'client' => [
                'headers' => $headers,
            ],
        ];

        $results = $this->doScan($searchParams);

        yield from $results;
    }

    protected function convertEsHitToDocument(
        array $hit,
        bool $adaptScoreForEmbedding = false,
        bool $scaleScore = true
    ): Document {
        try {
            $metaData = [];

            foreach ($hit['_source'] as $k => $v) {
                if (in_array($k, [$this->contentField, "contentType", "idHashKeys", $this->embeddingField])) {
                    $metaData[$k] = $v;
                }
            }

            $name = Arr::pull($metaData, $this->nameField);

            if ($name) {
                $metaData['name'] = $name;
            }

            if ($highlight = Arr::get($hit, 'highlight')) {
                $metaData['highlighted'] = $hit['highlight'];
            }

            $score = $hit['_score'];

            if ($score) {
                if ($adaptScoreForEmbedding) {
                    $score = $this->getRawSimilarityScore($score);
                }

                if ($scaleScore) {
                    if ($adaptScoreForEmbedding) {
                        $score = $this->scaleToUnitInterval($score, $this->similarity);
                    } else {
                        $score = (float) Special::sigmoid($score / 8);
                    }
                }
            }

            $embedding = Arr::get($hit['_source'], $this->embeddingField);

            $document = Document::make([
                "id" => $hit["_id"],
                "content" => Arr::get($hit["_source"], $this->contentField),
                "contentType" => Arr::get($hit["_source"], 'contentType'),
                "idHashKeys" => Arr::get($hit["_source"], 'idHashKeys'),
                "meta" => $metaData,
                "score" => $score,
                "embedding" => $embedding,
            ]);

            return $document;
        } catch (Throwable $exception) {
            dd($exception);
        }
    }
}
