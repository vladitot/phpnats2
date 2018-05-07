<?php
/**
 * Created by PhpStorm.
 * User: vladimirtot
 * Date: 18.04.2018
 * Time: 17:02
 */
set_time_limit(0);

use Nats\MessageBroker;
use Symfony\Component\Dotenv\Dotenv;

include dirname(__FILE__).'/../vendor/autoload.php';

$connectionOptions = MessageBroker::createConnectionOptionsFromEnv(dirname(__FILE__).'/.env');

try {
    $broker = new MessageBroker($connectionOptions);
} catch (Exception $e) {
    echo 'Problem with connection';
}

try {
    $broker->publishMessage($argv[1], $argv[2]);
} catch (Exception $e) {
    echo $e->getMessage();
}
