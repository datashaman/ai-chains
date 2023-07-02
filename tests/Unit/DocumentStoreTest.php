<?php

namespace Tests\Unit;

use App\Schema\Answer;
use App\Schema\Document;
use App\Schema\Label;
use App\Stores\OpensearchStore;
use App\Stores\Store;
use Illuminate\Support\Arr;
use Tests\TestCase;

class DocumentStoreTest extends TestCase
{
    protected array $documents;
    protected Store $ds;
    protected array $labels;
    protected string $indexName;

    public function setUp(): void
    {
        parent::setUp();

        $this->indexName = 'document-store-test';

        $this->documents = $this->getDocuments();
        $this->ds = $this->getDatastore();
        $this->labels = $this->getLabels($this->documents);
    }

    public function testWriteDocuments()
    {
        $this->ds->writeDocuments($this->documents);

        $docs = $this->ds->getAllDocuments();

        $this->assertEquals(count($docs), count($this->documents));

        $this->assertEquals(
            Arr::pluck($this->documents, 'id'),
            Arr::pluck($docs, 'id')
        );
    }

    public function testWriteLabels()
    {
        $this->ds->writeLabels($this->labels);

        $this->assertEquals($this->labels, $this->ds->getAlllabels());
    }

    protected function getDatastore(): Store
    {
        return new OpenSearchStore(
            documents: $this->documents,
            index: $this->indexName,
            labelIndex: "{$this->indexName}_labels",
            hosts: config('services.opensearch.hosts'),
            createIndex: true,
            recreateIndex: true
        );
    }

    protected function getDocuments(): array
    {
        $documents = [];

        for ($i = 0; $i < 3; $i++) {
            $documents[] = new Document(
                content: "A Foo Document {$i}",
                meta: [
                    "name" => "name_{$i}",
                    "year" => "2020",
                    "month" => "01",
                    "numbers" => [2, 4],
                ],
                embedding: $this->randomFloats(),
            );

            $documents[] = new Document(
                content: "A Bar Document {$i}",
                meta: [
                    "name" => "name_{$i}",
                    "year" => "2021",
                    "month" => "02",
                    "numbers" => [-2, -4],
                ],
                embedding: $this->randomFloats(),
            );

            $documents[] = new Document(
                content: "Document {$i} without embeddings",
                meta: [
                    "name" => "name_{$i}",
                    "noEmbedding" => true,
                    "month" => "03",
                ],
            );
        }

        return $documents;
    }

    protected function getLabels(array $documents): array
    {
        $labels = [];

        foreach ($documents as $i => $document) {
            $labels[] = new Label(
                query: "query_{$i}",
                document: $document,
                isCorrectDocument: true,
                isCorrectAnswer: false,
                origin: $i % 2 ? 'user-feedback' : 'gold-label',
                answer: $i ? new Answer("the answer is {$i}", documentIds: [$document->id]) : null,
                meta: [
                    'name' => "label_{$i}",
                    'year' => (string) (2020 + $i),
                ],
            );
        }

        return $labels;
    }

    protected function randomFloat(int $min, int $max): float
    {
        return $min + lcg_value() * abs($max - $min);
    }

    protected function randomFloats(int $number = 768, int $min = -1, int $max = 1): array
    {
        return array_map(
            fn () => $this->randomFloat($min, $max),
            array_pad([], $number, null)
        );
    }
}
