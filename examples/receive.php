<?php
/**
 * Created by PhpStorm.
 * User: vladimirtot
 * Date: 18.04.2018
 * Time: 17:02
 */
set_time_limit(0);

use Nats\MessageBroker;

include dirname(__FILE__).'/../vendor/autoload.php';

try {
    MessageBroker::setConfig(dirname(__FILE__).'/.env');
    $broker = MessageBroker::getInstance();
} catch (Exception $e) {
    exit('Problem with connection');
}

$broker->reSubscribeTo($argv[1]);

while (true) {
    try {
        if ($message = $broker->waitForOneMessage($argv[1])) {
            echo $message."\n\n";
        }
    } catch (Exception $e) {
        exit($e->getMessage());
    }
}
