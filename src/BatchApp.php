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
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\Yaml\Yaml;

class BatchApp
{
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
        $this->sleepSeconds = $batch['sleepSeconds'];
    }

    public function run(): void
    {
        $this->app->getContainer()->register(BatchApp::class)->setSynthetic(true);
        $this->app->getContainer()->set(BatchApp::class, $this);
        $this->app->compile();

        $this->log->notice("Allegro batch {$this->appName} started, sleep interval {$this->sleepSeconds} seconds");

        while (true) {
            if ($this->app->isTermSignalReceived()) {
                $this->log->info('Performing graceful shutdown on SIGTERM');
                break;
            }

            /** @var BatchInterface $task */
            $task = $this->app->getContainer()->get($this->handlerName);

            $rerun = $task->run();
            $this->app->ping();

            if ($rerun) {
                continue;
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
