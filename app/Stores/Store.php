<?php

namespace App\Stores;

use Datashaman\JobChain\HasJobChain;
use Illuminate\Bus\Queueable;
use Illuminate\Contracts\Queue\ShouldQueue;
use Illuminate\Foundation\Bus\Dispatchable;
use Illuminate\Queue\InteractsWithQueue;
use Illuminate\Queue\SerializesModels;
use Illuminate\Support\Arr;

abstract class Store implements ShouldQueue
{
    use Dispatchable;
    use InteractsWithQueue;
    use HasJobChain;
    use Queueable;
    use SerializesModels;

    protected string $index;
    protected string $labelIndex;
    protected string $similarity;
    protected array $duplicateDocumentsOptions = [
        'skip',
        'overwrite',
        'fail',
    ];
    protected $idsIterator = null;

    abstract public function getAllLabels(
        ?string $index = null,
        ?array $filters = null,
        ?array $headers = null
    ): array;

    protected function handleDuplicateDocuments(
        array $documents,
        ?string $index = null,
        ?string $duplicateDocuments = null,
        ?array $headers = null
    ) {
        $index = $index ?: $this->index;

        if (in_array($duplicateDocuments, ['skip', 'fail'])) {
            $documents = $this->dropDuplicateDocuments($documents, $index);
            $documentsFound = $this->getDocumentsById(
                ids: array_map(
                    fn ($d) => $d->id,
                    $documents
                ),
                index: $index,
                headers: $headers
            );
            $idsExistInDb = array_map(
                fn ($d) => $d->id,
                $documentsFound
            );

            if ($idsExistInDb && $duplicateDocuments === "fail") {
                $ids = implode(', ', $idsExistInDb);

                throw new DuplicateDocumentError(
                    "Document with ids '{$ids} already exists in index = '{$index}'."
                );
            }

            $documents = array_filter(
                $documents,
                fn ($doc) => ! in_array($doc->id, $idsExistInDb)
            );
        }

        return $documents;
    }

    protected function getDuplicateLabels(
        array $labels,
        ?string $index = null,
        ?array $headers = null
    ) {
        $index = $index ?: $this->index;
        $newIds = array_map(
            fn ($label) => $label->id,
            $labels
        );
        $duplicateIds = [];

        foreach (array_count_values($newIds) as $labelId => $count) {
            if ($count > 1) {
                $duplicateIds[] = $labelId;
            }
        }

        foreach ($this->getAllLabels(index: $index, headers: $headers) as $label) {
            if (in_array($label->id, $newIds)) {
                $duplicateIds[] = $label->id;
            }
        }

        return array_filter(
            $labels,
            fn ($label) => in_array($label->id, $duplicateIds),
        );
    }
}
