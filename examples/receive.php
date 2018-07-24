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
    exit('Problem with connection'. $e->getMessage());
}

$broker->subscribeToSubject($argv[1]);



while (true) {
    try {
        $broker->client->wait(1);
        echo $broker->getMessage();
    } catch (Exception $e) {
        exit($e->getMessage());
    }
}
