<?php
/**
 * Created by PhpStorm.
 * User: vladimirtot
 * Date: 18.04.2018
 * Time: 14:18
 */
namespace Nats;

use Exception;
use Symfony\Component\Dotenv\Dotenv;

/**
 * Класс обеспечения связи с сервером сообщений
 * Class MessageBroker
 */
class MessageBroker
{
    /** @var \Nats\Connection */
    public $client;
    private $messages = [];


    /**
     * MessageBroker constructor.
     * @throws Exception
     */
    public function __construct($connectionOptions)
    {
        $client = new \Nats\Connection($connectionOptions);
        try {
            $client->connect(30);
            $this->client = $client;
        } catch (\Exception $e) {
            throw new Exception('Ошибка подключения к серверу сообщений', $e->getCode(), $e);
        }
    }

    /**
     * Отправить сообщение в канал
     * @param $subject
     * @param $message
     * @throws Exception
     */
    public function publishMessage($subject, $message) {
        try {
            $this->client->publish($subject, $message);
        } catch (\Nats\Exception $e) {
            throw new Exception('Ошибка отправки сообщения', $e->getCode(), $e);
        }
    }

    /**
     * Подписывается на канал в режиме очереди (сообщение получит только один рандомный участник очереди)
     * @param $channel
     * @param $queue
     */
    public function subscribeToQueue($channel, $queue) {
        $this->client->queueSubscribe(
            $channel,
            $queue,
            function ($message) {
                $this->messages[] = $message;
            }
        );
    }

    /** Подписаться на канал
     * @param $channel
     */
    public function subscribeToSubject($channel) {
        $this->client->subscribe(
            $channel,
            function ($message) {
                $this->messages[] = $message;
            }
        );
    }

    /**
     * Получить последнее сообщение.
     * хз. как оно будет себя вести при наличиии коллбеков, поэтому получаю наверняка конкретный элемент
     *
     * @return mixed
     */
    private function getMessage()
    {
        if (count($this->messages)==0) return null;
        $keys = array_keys($this->messages);
        $message = $this->messages[$keys[count($keys)-1]];
        unset($this->messages[$keys[count($keys)-1]]);
        return $message;
    }

    /**
     * Ожидает одного сообщения и возвращает его
     * @return mixed
     */
    public function waitForOneMessage($subject, $queueGroup=null) {
        $newMessage=null;
        while (true) {
            $this->client->ping();
            $this->client->wait(1);
            $newMessage = $this->getMessage();
            if ($newMessage!==null) {
                break;
            } else {
                $this->client->reconnect();
                $this->reSubscribeTo($subject, $queueGroup);
                echo 'reconnected'."\n";
            }
        }
        return $newMessage;
    }

    /**
     * Подключиться или переподключиться к каналу в обычном режиме или в режие очереди
     * @param $subject
     * @param null $queueGroup
     */
    public function reSubscribeTo($subject, $queueGroup=null) {
        if ($queueGroup==null) {
            $prefix = $subject;
        } else {
            $prefix = $subject . '-' . $queueGroup;
        }

        foreach ($this->client->getSubscriptions() as $subscriptionName) {
            if (stristr($subscriptionName, $prefix)===0) {
                $this->client->unsubscribe($subscriptionName);
                break;
            }
        }

        if ($queueGroup!=null) {
            $this->subscribeToQueue($subject, $queueGroup);
        } else {
            $this->subscribeToSubject($subject);
        }
    }


    /**
     * Отправить короткое сообщение в канал и закрыть его за собой
     * @param $subject
     * @param $message
     * @return bool
     */
    public static function fastMessageToSubject($subject, $message) {
        try {
            $broker = new self(self::createConnectionOptionsFromEnv());
        } catch (Exception $e) {
            echo 'Problem with connection';
            return false;
        }
        try {
            $broker->publishMessage($subject, $message);
        } catch (Exception $e) {
            echo 'Problem with sending to debug';
            return false;
        }
        $broker->client->close();
        return true;
    }

    /**
     * Создать параметры подключения
     * @param null $env
     * @return ConnectionOptions
     */
    public static function createConnectionOptionsFromEnv($env=null) {
        if ($env==null) {
            $env=__DIR__.'/../.env';
        }
        $dotenv = new Dotenv();
        $dotenv->load($env);
        return new \Nats\ConnectionOptions(
            [
                'user'=>getenv('USER'),
                'pass'=>getenv('PASS'),
                'host'=>getenv('HOST'),
                'token'=>getenv('TOKEN')
            ]
        );
    }
}