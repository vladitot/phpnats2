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
 * Class of communication with the message server
 * Class MessageBroker
 */
class MessageBroker
{
    /** @var \Nats\Connection */
    public $client;

    protected $messages = [];

    private static $_instance = null;
    public static $connectionOption;
    /**
     * Set the path to the configuration file
     *
     * @param $env
     */
    public static function setConfig($env)
    {
        $dotenv = new Dotenv();
        $dotenv->load($env);
        self::setConnectionOption(new \Nats\ConnectionOptions(
            [
                'user' => getenv('USER'),
                'pass' => getenv('PASS'),
                'host' => getenv('HOST'),
                'token' => getenv('TOKEN')
            ]
        ));
    }

    /**
     * Creates or returns an instance of an object
     *
     * @return MessageBroker
     * @throws Exception
     */
    public static function getInstance()
    {
        if (is_null(self::$_instance)) {
            self::$_instance = new self();
        }
        return self::$_instance;
    }

    /**
     * MessageBroker constructor.
     *
     * @throws Exception
     */
    private function __construct()
    {
        $client = new \Nats\Connection(self::$connectionOption);
        try {
            $client->connect(30);
            $this->client = $client;
        } catch (\Exception $e) {
            throw new Exception($e->getMessage(), $e->getCode(), $e);
        }
    }

    /**
     * @param mixed $connectionOption
     */
    protected static function setConnectionOption($connectionOption)
    {
        self::$connectionOption = $connectionOption;
    }

    private function __wakeup()
    {

    }

    private function __clone()
    {

    }

    /**
     * Send a message to the channel
     *
     * @param $subject
     * @param $message
     * @throws Exception
     */
    public function publishMessage($subject, $message)
    {
        try {
            $this->client->publish($subject, $message);
        } catch (\Nats\Exception $e) {
            throw new Exception('Error sending message', $e->getCode(), $e);
        }
    }

    /**
     * Subscribe to a channel as a queue member
     *      (only one random participant of the queue will receive the message)
     *
     * @param $subject
     * @param $queue
     * @throws Exception
     */
    public function subscribeToQueue($subject, $queue)
    {
        $this->client->queueSubscribe(
            $subject,
            $queue,
            function ($message) use ($subject) {
                /** @var Message $message */
                $message->setOriginalSubject($subject);
                $this->messages[$subject][] = $message;
            }
        );
    }

    /**
     * Subscribe to a channel
     *
     * @param $subject
     * @throws Exception
     */
    public function subscribeToSubject($subject)
    {
        $this->client->subscribe(
            $subject,
            function ($message) use ($subject) {
                /** @var Message $message */
                $message->setOriginalSubject($subject);
                $this->messages[$subject][] = $message;
            }
        );
    }

    /**
     * Get latest message
     *
     * @param null $channel
     * @return Message
     */
    public function getMessage($channel=null)
    {
        if ($channel!=null && is_array($this->messages[$channel])) {
            if (count($this->messages[$channel]) == 0) {
                return null;
            }
            $keys = array_keys($this->messages[$channel]);
            $message = $this->messages[$channel][$keys[count($keys) - 1]];
            unset($this->messages[$channel][$keys[count($keys) - 1]]);
            return $message;
        } else {
            $message = null;
            foreach ($this->messages as $channel=>&$messagesPack) {
                if (count($messagesPack) == 0) {
                    continue;
                }
                $keys = array_keys($messagesPack);
                $message = $messagesPack[$keys[count($keys) - 1]];
                unset($messagesPack[$keys[count($keys) - 1]]);
            }
            return $message;
        }
    }

    /**
     * Waits for one message and returns it
     *
     * @return mixed
     * @throws Exception
     */
    public function waitForOneMessage($subject, $queueGroup = null)
    {
        $newMessage = null;
        while (true) {
            $this->client->wait(1);
            $newMessage = $this->getMessage();
            if ($newMessage !== null) {
                break;
            } else {
                $this->client->reconnect();
                $this->reSubscribeTo($subject, $queueGroup);
            }
        }
        return $newMessage;
    }

    /**
     * Connect or reconnect to the channel in normal mode or in the queue mode
     *
     * @param $subject
     * @param null $queueGroup
     * @throws Exception
     */
    public function reSubscribeTo($subject, $queueGroup = null)
    {
        if ($queueGroup == null) {
            $prefix = $subject;
        } else {
            $prefix = $subject . '-' . $queueGroup;
        }

        foreach ($this->client->getSubscriptions() as $subscriptionName) {
            if (stristr($subscriptionName, $prefix) === 0) {
                $this->client->unsubscribe($subscriptionName);
                break;
            }
        }

        if ($queueGroup != null) {
            $this->subscribeToQueue($subject, $queueGroup);
        } else {
            $this->subscribeToSubject($subject);
        }
    }


    /**
     * Send a short message to the channel and close it
     *
     * @param $subject
     * @param $message
     * @return bool
     */
    public static function fastMessageToSubject($subject, $message)
    {
        try {
            $broker = new self();
        } catch (Exception $e) {
            echo 'Problem with connection';
            return false;
        }
        $return = false;
        try {
            $broker->publishMessage($subject, $message);
            $return = true;
        } catch (Exception $e) {
            echo 'Problem with sending to debug';
        } finally {
            $broker->client->close();
        }
        return $return;
    }

    /**
     * Send request to the channel
     * @param $subject
     * @param $message
     * @return mixed
     * @throws Exception
     */
    public function publishRequest($subject, $message)
    {
        try {
            $this->client->request($subject, $message, function ($response) use ($subject) {
                /** @var Message $response */
                $response->setOriginalSubject($subject);
                $this->messages[$subject][] = $response;
            });
        } catch (\Nats\Exception $e) {
            throw new Exception('Error sending message', $e->getCode(), $e);
        }
        return $subject;
    }
}