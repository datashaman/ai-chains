<?php

namespace App\Jobs;

use App\Document;
use Illuminate\Support\Facades\Log;

class TextConverterJob extends Job
{
    public function __construct(
        protected array $filePaths,
        protected bool $removeNumericTables = false,
        protected ?array $validLanguages = null,
    ) {
    }

    public function handle()
    {
        $documents = [];

        foreach ($this->filePaths as $filePath) {
            $documents = array_merge(
                $documents,
                $this->convert($filePath)
            );
        }

        $this->done(null, $documents);
    }

    public function convert(
        string $filePath,
        ?array $meta = null,
        ?bool $removeNumericTables = null,
        ?array $validLanguages = null,
        string $encoding = 'utf-8'
    ): array {
        if (is_null($removeNumericTables)) {
            $removeNumericTables = $this->removeNumericTables;
        }

        if (is_null($validLanguages)) {
            $validLanguages = $this->validLanguages;
        }

        $text = file_get_contents($filePath);
        $pages = explode('\f', $text);

        foreach ($pages as $page) {
            $lines = preg_split("/\r\n|\n|\r/", $page);
            $cleanedLines = [];

            foreach ($lines as $line) {
                $words = preg_split("/\s*/", $line);
                $digits = array_filter(
                    $words,
                    fn ($word) => preg_match("/[0-9]/", $word)
                );

                if ($removeNumericTables) {
                    if ($words && count($digits) / count($words) > 0.4 && !Str::endsWith(trim($line), '.')) {
                        Log::debug("Removing line '{$line}' from {$filePath}");
                        continue;
                    }
                }

                $cleanedLines[] = $line;
            }

            $page = implode("\n", $cleanedLines);
            $cleanedPages[] = $page;
        }

        if ($validLanguages) {
            $documentText = implode('', $cleanedPages);

            if (!$this->validateLanguage($documentText, $validLanguages)) {
                $languages = implode(', ', $validLanguages);
                Log::warning(
                    "The language for {$filePath} is not one of {$languages}. The file may not have been decoded in the correct text format."
                );
            }
        }

        $text = implode('', $cleanedPages);

        $document = new Document(
            content: $text,
            contentType: 'text'
        );

        return [$document];
    }

}
