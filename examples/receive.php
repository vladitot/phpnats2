<?php
/**
 * Created by PhpStorm.
 * User: vladimirtot
 * Date: 18.04.2018
 * Time: 17:02
 */
set_time_limit(0);

//use Joli\JoliNotif\Notification;
//use Joli\JoliNotif\NotifierFactory;
use Nats\MessageBroker;
use Symfony\Component\Dotenv\Dotenv;

include dirname(__FILE__).'/../vendor/autoload.php';

$connectionOptions = MessageBroker::createConnectionOptionsFromEnv(dirname(__FILE__).'/.env');

try {
    $broker = new MessageBroker($connectionOptions);
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
