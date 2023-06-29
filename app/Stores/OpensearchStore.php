<?php

namespace App\Stores;

use App\Exceptions\ConnectionError;
use App\Exceptions\DocumentStoreError;
use Illuminate\Support\Arr;
use Illuminate\Support\Facades\Log;
use OpenSearch\Client;
use OpenSearch\ClientBuilder;
use OpenSearch\Common\Exceptions\Missing404Exception;
use OpenSearch\Common\Exceptions\NoNodesAvailableException;
use OpenSearch\Helper\Iterators\SearchResponseIterator;
use Throwable;
use ValueError;

class OpensearchStore extends SearchEngineStore
{
    const SIMILARITY_SPACE_TYPE_MAPPINGS = [
        "nmslib" => [
            "cosine" => "cosinesimil",
            "dot_product" => "innerproduct",
            "l2" => "l2",
        ],
        "score_script" => [
            "cosine" => "cosinesimil",
            "dot_product" => "innerproduct",
            "l2" => "l2",
        ],
        "faiss" => [
            "cosine" => "innerproduct",
            "dot_product" => "innerproduct",
            "l2" => "l2"
        ],
    ];

    protected array $validIndexTypes = [
        "flat",
        "hnsw",
        "ivf",
        "ivf_pq",
    ];

    protected ?string $spaceType = null;

    protected $client;

    public function __construct(
        protected array $documents,
        protected string $index = 'document',
        protected string|array $hosts = 'https://127.0.0.1:9200',
        protected string $username = 'admin',
        protected string $password = 'admin',
        protected string $labelIndex = 'label',
        protected array $searchFields = ['content'],
        protected string $contentField = 'content',
        protected string $nameField = 'name',
        protected string $embeddingField = 'embedding',
        protected int $embeddingDim = 768,
        protected ?array $customMapping = null,
        protected bool $verifyCerts = false,
        protected bool $createIndex = false,
        protected string $refreshType = 'wait_for',
        protected bool $recreateIndex = false,
        protected string $similarity = 'dot_product',
        protected bool $returnEmbedding = false,
        protected string $duplicateDocuments = 'overwrite',
        protected string $indexType = 'flat',
        protected string $scroll = '1d',
        protected ?array $synonyms = null,
        protected string $synonymType = 'synonym',
        protected string $knnEngine = "nmslib",
        protected ?array $knnParameters = null,
        protected ?int $ivfTrainSize = null,
        protected int $batchSize = 10_000
    ) {
        $this->hosts = (array) $hosts;
        $this->client = $this->getClient();

        try {
            $this->client->indices()->get([
                'index' => $index,
            ]);
        } catch (Missing404Exception $exception) {
            // Ignore it
        } catch (NoNodesAvailableException $exception) {
            $hosts = implode(',', $hosts);

            throw new ConnectionError(
                "Initial connection to Opensearch failed with error '{$exception->getMessage()}'\n"
                . "Make sure an Opensearch instance is running at `{$hosts}` and that it has finished booting (can take > 30s)."
            );
        }

        if (!in_array($knnEngine, ['nmslib', 'faiss', 'score_script'])) {
            throw new ValueError("knn_engine must be either 'nmslib', 'faiss' or 'score_script' but was {$knnEngine}");
        }

        if (in_array($indexType, $this->validIndexTypes)) {
            if (in_array($indexType, ["ivf", "ivf_pq"]) && $knnEngine !== "faiss") {
                throw new DocumentStoreError("Use 'faiss' as knn_engine when using 'ivf' as index_type.");
            }
            $this->indexType = $indexType;
        } else {
            throw new DocumentStoreError(
                "Invalid value for indexType in constructor. Choose one of these values: {$this->validIndexTypes}."
            );
        }

        $this->knnEngine = $knnEngine;
        $this->knnParameters = $this->knnParameters ?? [];

        if ($ivfTrainSize) {
            if ($ivfTrainSize <= 0) {
                throw new DocumentStoreError("`ivf_train_on_write_size` must be None or a positive integer.");
            }
            $this->ivfTrainSize = $ivfTrainSize;
        } elseif (in_array($this->indexType, ['ivf', 'ivf_pq'])) {
            $this->ivfTrainSize = $this->recommendedIvfTrainSize();
        }
        $this->spaceType = static::SIMILARITY_SPACE_TYPE_MAPPINGS[$knnEngine][$similarity];

        parent::__construct(
            documents: $documents,
            client: $this->client,
            index: $index,
            labelIndex: $labelIndex,
            searchFields: $searchFields,
            contentField: $contentField,
            nameField: $nameField,
            embeddingField: $embeddingField,
            embeddingDim: $embeddingDim,
            customMapping: $customMapping,
            recreateIndex: $recreateIndex,
            createIndex: $createIndex,
            similarity: $similarity,
            returnEmbedding: $returnEmbedding,
            duplicateDocuments: $duplicateDocuments,
            synonyms: $synonyms,
            synonymType: $synonymType,
            batchSize: $batchSize
        );
    }

    public function handle()
    {
        $response = $this->writeDocuments(
            $this->documents
        );

        $this->done(null, $response);
    }

    public function writeDocuments(
        array $documents,
        ?string $index = null,
        ?int $batchSize = null,
        ?string $duplicateDocuments = null,
        ?array $headers = null
    ) {
        if ($index && !$this->indexExists($index)) {
            $this->createDocumentIndex($index);
        }

        $index = $index ?: $this->index;
        $batchSize = $batchSize ?: $this->batchSize;

        parent::writeDocuments(
            documents: $documents,
            index: $index,
            batchSize: $batchSize,
            duplicateDocuments: $duplicateDocuments,
            headers: $headers
        );
    }

    protected function getClient(): Client
    {
        return (new ClientBuilder())
            ->setHosts($this->hosts)
            ->setBasicAuthentication(
                $this->username,
                $this->password
            )
            ->setSSLVerification($this->verifyCerts)
            ->build();
    }

    protected function deleteIndex(string $index): void
    {
        if ($this->indexExists($index)) {
            $indexInfo = $this->client->indices()->get([
                'index' => $index,
            ]);
            $indexMapping = $indexInfo[$index]['mappings']['properties'];

            if ($modelId = Arr::get($indexMapping, "{$this->embeddingField}.model_id")) {
                $this->client->transport()->performRequest(
                    'DELETE',
                    "/_plugins/_knn/models/{$modelId}"
                );
            }
        }

        parent::deleteIndex($index);
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
            $this->deleteIvfModel($index);
            Log::info("Index '{$index}' deleted.");
        }
    }

    protected function deleteIvfModel(string $index)
    {
        if ($this->indexExists(".opensearch-knn-models")) {
            $response = $this->client->transport()->performRequest("GET", "/_plugins/_knn/models/_search");
            $existingIvfModels = array_unique(
                array_map(
                    fn ($model) => $model['_source']['model_id'],
                    $response['hits']['hits']
                )
            );
            if (in_array("{$index}-ivf", $existingIvfModels)) {
                $this->client->transport()->performRequest("DELETE", "/_plugins/_knn/models/{$index}-ivf");
            }
        }
    }

    protected function doBulk(
        array $documents,
        string $refresh = 'wait_for',
        ?array $headers = null
    ) {
        return $this->client->bulk([
            'index' => $this->index,
            'body' => $this->formatBulk($documents),
            'refresh' => $refresh,
            'client' => [
                'headers' => $headers,
            ],
        ]);
    }

    protected function formatBulk(
        array $documents
    ): array {
        $bulk = [];

        foreach ($documents as $document) {
            $operation = $this->duplicateDocuments ? 'index' : 'create';
            $bulk[] = [ $operation => [
                '_index' => $this->index,
                '_id' => Arr::pull($document, 'id'),
            ]];
            $bulk[] = $document;
        }

        return $bulk;
    }

    protected function doScan($searchParams)
    {
        $iterator = new SearchResponseIterator($this->client, $searchParams);

        foreach ($iterator as $page) {
            yield from $page['hits']['hits'];
        }
    }

    protected function createDocumentIndex(
        string $indexName,
        ?array $headers = null
    ) {
        if ($this->customMapping) {
            $indexDefinition = $this->customMapping;
        } else {
            $indexDefinition = [
                'mappings' => [
                    'properties' => [
                        $this->nameField => [
                            'type' => 'keyword',
                        ],
                        $this->contentField => [
                            'type' => 'text',
                        ],
                    ],
                    "dynamic_templates" => [
                        [
                            "strings" => [
                                "path_match" => "*",
                                "match_mapping_type" => "string",
                                "mapping" => [
                                    "type" => "keyword",
                                ],
                            ],
                        ],
                    ],
                ],
            ];

            if ($this->synonyms) {
                foreach ($this->searchFields as $field) {
                    $indexDefinition['mappings']['properties'] = array_merge(
                        $indexDefinition['mappings']['properties'],
                        [
                            $field => [
                                'type' => 'text',
                                'analyzer' => 'synonym',
                            ],
                        ]
                    );

                    $indexDefinition['mappings']['properties'][$this->contentField] = [
                        'type' => 'text',
                        'analyzer' => 'synonym',
                    ];

                    $indexDefinition["settings"]["analysis"]["analyzer"]["synonym"] = [
                        "tokenizer" => "whitespace",
                        "filter" => ["lowercase", "synonym"],
                    ];

                    $indexDefinition["settings"]["analysis"]["filter"] = [
                        "synonym" => [
                            "type" => $this->synonymType,
                            "synonyms" => $this->synonyms,
                        ],
                    ];
                }
            } else {
                foreach ($this->searchFields as $field) {
                    $indexDefinition['mappings']['properties'] = array_merge(
                        $indexDefinition['mappings']['properties'],
                        [
                            $field => [
                                'type' => 'text',
                            ],
                        ]
                    );
                }
            }

            if ($this->embeddingField) {
                $indexDefinition['settings']['index'] = [
                    'knn' => true,
                ];

                if ($this->knnEngine === "nmslib" && $this->indexType == "hnsw") {
                    $efSearch = $this->getEfSearchValue();
                    $indexDefinition["settings"]["index"]["knn.algo_param.ef_search"] = $efSearch;
                }

                $indexDefinition["mappings"]["properties"][$this->embeddingField] = $this->getEmbeddingFieldMapping(index: $indexName);
            }
        }

        try {
            $this->client->indices()->create([
                'index' => $indexName,
                'body' => $indexDefinition,
                'client' => [
                    'headers' => $headers,
                ],
            ]);
        } catch (Throwable $exception) {
            throw new ConnectionError(
                "Initial connection to Opensearch failed with error '{$exception->getMessage()}'\n"
                . "Make sure an Opensearch instance is running at `{$hosts}` and that it has finished booting (can take > 30s).",
                0,
                $exception
            );
        }
    }

    protected function getEmbeddingFieldMapping(
        ?string $knnEngine = null,
        ?string $spaceType = null,
        ?string $indexType = null,
        ?int $embeddingDim = null,
        ?string $index = null
    ) {
        $spaceType = $spaceType ?: $this->spaceType;
        $knnEngine = $knnEngine ?: $this->knnEngine;
        $indexType = $indexType ?: $this->indexType;
        $embeddingDim = $embeddingDim ?: $this->embeddingDim;
        $index = $index ?: $this->index;

        $embeddingsFieldMapping = [
            'type' => 'knn_vector',
            'dimension' => $embeddingDim,
        ];

        if ($knnEngine !== 'score_script') {
            $method = [
                'space_type' => $spaceType,
                'engine' => $knnEngine,
            ];

            $efConstruction = $this->knnParameters['ef_construction'] ?? 80;
            $efSearch = $this->getEfSearchValue();
            $m = $this->knnParameters['m'] ?? 64;

            switch ($indexType) {
                case 'flat':
                    $method['name'] = 'hnsw';
                    $method['parameters'] = [
                        'ef_construction' => 512,
                        'm' => 16,
                    ];
                    break;
                case 'hnsw':
                    $method['name'] = 'hnsw';
                    $method["parameters"] = [
                        "ef_construction" => $efConstruction,
                        "m" => $m,
                    ];

                    if ($knnEngine == "faiss") {
                        $method["parameters"]["ef_search"] = $efSearch;
                    }
                    break;
                case 'ivf':
                case 'ivf_pq':
                    if ($knnEngine != "faiss") {
                        throw new DocumentStoreError("To use 'ivf' or 'ivf_pq as index_type, set knn_engine to 'faiss'.");
                    }

                    if ($this->ivfModelExists($index)) {
                        Log::info("Using existing IVF model '{$index}-ivf' for index '{$index}'.");
                        $embeddingsFieldMapping = [
                            "type" => "knn_vector",
                            "model_id" => "{$index}-ivf",
                        ];
                        $method = [];
                    } else {
                        Log::info("Using index of type 'flat' for index '{$index}' until IVF model is trained.");
                        $method = [];
                    }
                    break;
                default:
                    Log::error("Set index_type to either 'flat', 'hnsw', 'ivf', or 'ivf_pq'.");
                    $method["name"] = "hnsw";
            }

            if ($method) {
                $embeddingsFieldMapping['method'] = $method;
            }
        }

        return $embeddingsFieldMapping;
    }

    protected function getEfSearchValue(): int
    {
        return $this->knnParameters['ef_search'] ?? 20;
    }

    protected function createLabelIndex(
        string $indexName,
        ?array $headers = null
    ) {
        $mapping = [
            "mappings" => [
                "properties" => [
                    "query" => [
                        "type" => "text",
                    ],
                    "answer" => [
                        "type" => "nested"
                    ],
                    "document" => [
                        "type" => "nested",
                    ],
                    "is_correct_answer" => [
                        "type" => "boolean",
                    ],
                    "is_correct_document" => [
                        "type" => "boolean",
                    ],
                    "origin" => [
                        "type" => "keyword",
                    ],
                    "document_id" => [
                        "type" => "keyword",
                    ],
                    "no_answer" => [
                        "type" => "boolean",
                    ],
                    "pipeline_id" => [
                        "type" => "keyword",
                    ],
                    "created_at" => [
                        "type" => "date",
                        "format" => "yyyy-MM-dd HH =>mm =>ss||yyyy-MM-dd||epoch_millis",
                    ],
                    "updated_at" => [
                        "type" => "date",
                        "format" => "yyyy-MM-dd HH =>mm =>ss||yyyy-MM-dd||epoch_millis"
                    ],
                ]
            ]
        ];

        $response = $this->client->indices()->create([
            'index' => $indexName,
            'body' => $mapping,
            'client' => [
                'headers' => $headers,
            ],
        ]);
    }

    protected function validateAndAdjustDocumentIndex(string $indexName, ?array $headers = null)
    {
        $indices = $this->client->indices()->get([
            'index' => $indexName,
            'client' => [
                'headers' => $headers,
                'ignore' => [404],
            ],
        ]);

        if (!$indices) {
            Log::warning(
                "Before you can use an index, you must create it first. The index '{$indexName}' doesn't exist. "
                . "You can create it by setting `createIndex=true` on construction or by calling `writeDocuments()` if you prefer to create it on demand. "
                . "Note that this instance doesn't validate the index after you created it.",
            );
        }

        foreach ($indices as $indexId => $indexInfo) {
            $mappings = $indexInfo['mappings'];
            $indexSettings = $indexInfo['settings']['index'];

            if ($this->searchFields) {
                foreach ($this->searchFields as $searchField) {
                    if (in_array($searchField, $mappings['properties'])) {
                        if ($mappings['properties'][$searchField]['type'] !== 'text') {
                            throw new DocumentStoreError(
                                "The index '{$indexId}' needs the 'text' type for the search_field '{$searchField}' to run full text search, but got type '{$mappings['properties'][$seaarchField]['type']}'. "
                                . "You can fix this issue in one of the following ways: "
                                . " - Recreate the index by setting `recreateIndex: True` (Note that you'll lose all data stored in the index.) "
                                . " - Use another index name by setting `index: 'my_index_name'`. "
                                . " - Remove '{$searchField}' from `searchFields`. "
                            );
                        }
                    } else {
                        $mappings['properties'][$searchField] = $this->synonyms
                            ? [
                                'type' => 'text',
                                'analyzer' => 'synonym',
                            ]
                            : [
                                'type' => 'text',
                            ];
                        $this->client->indices()->putMapping([
                            'index' => $indexId,
                            'body' => $mappings,
                            'client' => [
                                'headers' => $headers,
                            ],
                        ]);
                    }
                }
            }

            $existingEmbeddingField = Arr::get($mappings['properties'], $this->embeddingField);

            if (!$existingEmbeddingField) {
                $mappings['properties'][$this->embeddingField] = $this->getEmbeddingFieldMapping(index: $indexName);

                $this->client->indices()->putMapping([
                    'index' => $indexId,
                    'body' => $mappings,
                    'client' => [
                        'headers' => $headers,
                    ],
                ]);
            } else {
                if ($existingEmbeddingField["type"] !== "knn_vector") {
                    throw new DocumentStoreError(
                        "The index '{$indexId}' needs the 'knn_vector' type for the embedding_field '{$this->embeddingField}' to run vector search, but got type '{$mappings['properties'][$this->embeddingField]['type']}'. "
                        . "You can fix it in one of these ways: "
                        . " - Recreate the index by setting `recreate_index: true` (Note that you'll lose all data stored in the index.) "
                        . " - Use another index name by setting `index: 'my_index_name'`. "
                        . " - Use another embedding field name by setting `embedding_field: 'my_embedding_field_name'`. "
                    );
                }

                $trainingRequired = in_array($this->indexType, ["ivf", "ivf_pq"]) && !in_array("model_id", $existingEmbeddingField);

                if ($this->knnEngine !== "score_script" && !$trainingRequired) {
                    $this->validateApproximateKnnSettings($existingEmbeddingField, $indexSettings, $indexId);
                }
            }
        }
    }

    protected function validateApproximateKnnSettings(
        array $existingEmbeddingField,
        array $indexSettings,
        string $indexId
    ) {
        $method = Arr::get($existingEmbeddingField, 'method');

        if (in_array('model_id', $existingEmbeddingField)) {
            $embeddingFieldKnnEngine = 'faiss';
        } else {
            $embeddingFieldKnnEngine = Arr::get($method, 'engine', 'nmslib');
        }

        $embeddingFieldSpaceType = Arr::get($method, 'space_type', 'l2');

        if ($embeddingFieldKnnEngine !== $this->knnEngine) {
            throw new DocumentStoreError(
                "Existing embedding field '{$this->embeddingField}' of OpenSearch index '{$indexId}' has knn_engine "
                . "'{$embeddingFieldKnnEngine}', but knn_engine was set to '{$this->knnEngine}'.\n"
                . "To switch knn_engine to '{$this->knnEngine}' consider one of these options:\n"
                . " - Clone the embedding field in the same index, for example,  `cloneEmbeddingField(knnEngine: '{$this->knnEngine}', ...)`.\n"
                . " - Create a new index by selecting a different index name, for example, `index: 'my_new_{$this->knnEngine}_index'`.\n"
                . " - Overwrite the existing index by setting `recreateIndex: true`. Note that you'll lose all existing data. \n",
            );
        }
    }
}
