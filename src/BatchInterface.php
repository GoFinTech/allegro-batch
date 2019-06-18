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


interface BatchInterface
{
    /**
     * @return bool|null if TRUE, batch will re-run without sleep
     */
    public function run();
}
