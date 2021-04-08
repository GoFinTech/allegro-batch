<?php

/*
 * This file is part of the Allegro framework.
 *
 * (c) 2019-2021 Go Financial Technologies, JSC
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace GoFinTech\Allegro\Batch;


use GoFinTech\Allegro\AllegroApp;
use InvalidArgumentException;
use LogicException;
use Psr\Log\LoggerInterface;
use Symfony\Component\Config\FileLocator;
use Symfony\Component\Yaml\Yaml;

class BatchApp
{
    public const RUN_CONTINUOUSLY = 'continuous';
    public const RUN_ONCE = 'once';
    public const RUN_TO_COMPLETION = 'complete';
    public const RUN_TO_COMPLETION_SLOW = 'complete-slow';

    private static $ALLOWED_RUN_MODES = [
        self::RUN_CONTINUOUSLY,
        self::RUN_ONCE,
        self::RUN_TO_COMPLETION,
        self::RUN_TO_COMPLETION_SLOW,
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

    /**
     * BatchApp constructor.
     * @param string|AllegroApp $configSection config section name in batch.yml.
     *      Might be an AllegroApp instance for backward compatibility.
     * @param string|null $legacyConfigSection config section name if app instance is passed as the first argument
     */
    public function __construct($configSection, $legacyConfigSection = null)
    {
        $this->app = AllegroApp::resolveConstructorParameters("BatchApp", $configSection, $legacyConfigSection);
        $this->log = $this->app->getLogger();

        $this->loadConfiguration($this->app->getConfigLocator(), $configSection);
    }

    /**
     * Shorthand for instantiating a BatchApp with specified config and calling run().
     * @param string $configSection
     */
    public static function exec(string $configSection): void
    {
        $batch = new BatchApp($configSection);
        $batch->run();
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
        // The following seems to be a bug in Symfony DI where by default both are true
        if ($handlerDefinition->isPrivate() || !$handlerDefinition->isPublic()) {
            $handlerDefinition->setPublic(true);
        }

        $this->app->compile();
    }

    public function run(): void
    {
        $this->prepare();

        $this->log->notice("Allegro batch {$this->appName} started, mode={$this->runMode}");
        if ($this->runMode == self::RUN_CONTINUOUSLY || $this->runMode == self::RUN_TO_COMPLETION_SLOW) {
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
                if ($this->runMode == self::RUN_TO_COMPLETION_SLOW) {
                    $this->sleep();
                }
                continue;
            }
            else if ($this->runMode == self::RUN_TO_COMPLETION || $this->runMode == self::RUN_TO_COMPLETION_SLOW) {
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
