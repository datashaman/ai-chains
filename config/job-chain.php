<?php

return [
    /**
     * Job chain loader will search through these paths for chain files.
     */
    'paths' => [
        base_path('chains'),
    ],

    /**
     * Cache store use for holding chain state.
     */
    'cache' => env('JOB_CHAIN_CACHE', env('CACHE_DRIVER', 'file')),

    /**
     * Cache item lifetime. This must be longer than the total expected
     * run time for any chain.
     *
     * This can overridden per chain.
     */
    'lifetime' => env('JOB_CHAIN_LIFETIME', 60 * 60 * 24),
];
