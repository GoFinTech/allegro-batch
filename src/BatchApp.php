<?php

/*
 * This file is part of the Allegro framework.
 *
 * (c) 2019 Go Financial Technologies, JSC
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GoFinTech\Allegro\Batch;


use GoFinTech\Allegro\AllegroApp;
use LogicException;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\Yaml\Yaml;

class BatchApp
{
    public const RUN_CONTINUOUSLY = 'continuous';
    public const RUN_ONCE = 'once';
    public const RUN_TO_COMPLETION = 'complete';

    private static $ALLOWED_RUN_MODES = [
        self::RUN_CONTINUOUSLY,
        self::RUN_ONCE,
        self::RUN_TO_COMPLETION,
    ];

    /** @var AllegroApp */
    private $app;
    /** @var LoggerInterface */
    private $log;
    /** @var string */
    private $appName;
    /** @var string */
    private $handlerName;
    /** @var int */
    private $sleepSeconds;
    /** @var string */
    private $runMode;
    /** @var bool */
    private $doPing;

    public function __construct(AllegroApp $app, string $configSection)
    {
        $this->app = $app;
        $this->log = $app->getLogger();

        $this->loadConfiguration($app->getConfigLocator(), $configSection);
    }

    private function loadConfiguration(FileLocator $locator, string $configSection): void
    {
        $config = Yaml::parseFile($locator->locate('batch.yml'));

        $this->appName = $configSection;

        $batch = $config[$configSection];
        $this->handlerName = $batch['handler'];
        $this->sleepSeconds = $batch['sleepSeconds'] ?? 60;

        $this->runMode = $batch['mode'] ?? self::RUN_CONTINUOUSLY;
        if (false === array_search($this->runMode, static::$ALLOWED_RUN_MODES))
            throw new LogicException("Allegro batch {$this->appName} has invalid mode={$this->runMode} configured");

        if (isset($batch['ping'])) {
            $this->doPing = $batch['ping'];
        }
        else if ($this->runMode == self::RUN_ONCE) {
            $this->doPing = false;
        }
        else {
            $this->doPing = true;
        }
    }

    private function prepare(): void
    {
        $container = $this->app->getContainer();

        $container->register(BatchApp::class)->setSynthetic(true);
        $container->set(BatchApp::class, $this);

        $handlerDefinition = $container->getDefinition($this->handlerName);
        if (!$handlerDefinition->isPublic())
            $handlerDefinition->setPublic(true);

        $this->app->compile();
    }

    public function run(): void
    {
        $this->prepare();

        $this->log->notice("Allegro batch {$this->appName} started, mode={$this->runMode}");
        if ($this->runMode == self::RUN_CONTINUOUSLY) {
            $this->log->notice("Sleep interval: {$this->sleepSeconds} seconds");
        }

        while (true) {
            if ($this->app->isTermSignalReceived()) {
                $this->log->info('Performing graceful shutdown on SIGTERM');
                break;
            }

            /** @var BatchInterface $task */
            $task = $this->app->getContainer()->get($this->handlerName);

            $rerun = $task->run();

            if ($this->doPing)
                $this->app->ping();

            if ($this->runMode == self::RUN_ONCE) {
                break;
            }
            else if ($rerun) {
                continue;
            }
            else if ($this->runMode == self::RUN_TO_COMPLETION) {
                break;
            }
            else {
                $this->sleep();
            }
        }
    }

    /**
     * For extended sleep times we still try to do pings and check for SIGTERM.
     * TODO For very long sleeps this will drift and sleep longer than requested
     */
    private function sleep(): void
    {
        $toSleep = $this->sleepSeconds;
        $toPing = 0;
        while ($toSleep > 10 && !$this->app->isTermSignalReceived()) {
            sleep(10);
            $toSleep -= 10;
            $toPing++;
            if ($toPing == 4) {
                $this->app->ping();
                $toPing = 0;
            }
        }
        sleep($toSleep);
    }
}
