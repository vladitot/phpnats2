<?php
/**
 * Created by PhpStorm.
 * User: vladimirtot
 * Date: 18.04.2018
 * Time: 17:02
 */
set_time_limit(0);

use Nats\MessageBroker;

include dirname(__FILE__) . '/../vendor/autoload.php';

try {
    MessageBroker::setConfig(dirname(__FILE__) . '/.env');
    $broker = MessageBroker::getInstance();
} catch (Exception $e) {
    exit('Problem with connection');
}

try {
    //Отправка запроса и ожидание ответа
    $channelForWaitResult = $broker->publishRequest($argv[1], 'Request: '.$argv[2]);
    $response = $broker->getMessage($channelForWaitResult);
    echo $response;

    //Отправка сообщения в один конец
    $broker->publishMessage($argv[1], 'Message :'.$argv[2]);
} catch (Exception $e) {
    echo $e->getMessage();
}
