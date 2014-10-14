<?php
namespace InfluxDB\Adapter;

use InfluxDB\Options;
use Zend\Http\Client;
use Zend\Json\Encoder ;
class ZendAdapter implements AdapterInterface, QueryableInterface
{
    private $httpClient;
    private $options;

    public function __construct(Client $httpClient, Options $options)
    {
    	if(!$httpClient->getUri()){
    		$endpoint = $this->options->getHttpSeriesEndpoint();
    		$httpClient->setUri($endpoint);
    	}
        $this->httpClient = $httpClient;
        $this->options = $options;
    }

    public function getOptions()
    {
        return $this->options;
    }
    
    protected function getResponse(array $options){
    	$response = null;
    	$this->httpClient->setMethod('GET');
    	$this->httpClient->setParameterGet($options);
    	$resp = $this->httpClient->send();
    	if($resp->isOk()){
    		$response = $resp->getBody();
    	}
    	
    	return Encoder::encode($response);
    	 
    }
    protected function postRequest(array $options, $method = 'POST'){
    	$response = null;
    	$this->httpClient->setMethod($method);
    	$this->httpClient->setParameterGet($options);
    	$resp = $this->httpClient->send();
    	if($resp->isOk()){
    		$response = $resp->getBody();
    	}
    	 
    	return Encoder::encode($response);
    
    }
    public function send($message, $timePrecision = false)
    {
        $httpMessage = [
            "auth" => [$this->options->getUsername(), $this->options->getPassword()],
            "body" => json_encode($message)
        ];

        if ($timePrecision) {
            $httpMessage["query"]["time_precision"] = $timePrecision;
        }
        
        return $this->postRequest($httpMessage);
    }

    public function query($query, $timePrecision = false)
    {
        $options = [
            "auth" => [$this->options->getUsername(), $this->options->getPassword()],
            'query' => [
                "q" => $query,
            ]
        ];

        if ($timePrecision) {
            $options["query"]["time_precision"] = $timePrecision;
        }
             
       return $this->postRequest($options);       
    }

    public function getDatabases()
    {
        $options = [
            "auth" => [$this->options->getUsername(), $this->options->getPassword()],
        ];

   
        return $this->getResponse($options);
    }

    public function createDatabase($name)
    {
        $httpMessage = [
            "auth" => [$this->options->getUsername(), $this->options->getPassword()],
            "body" => json_encode(["name" => $name])
        ];

        $endpoint = $this->options->getHttpDatabaseEndpoint();
         return $this->postRequest($httpMessage);
    }

    public function deleteDatabase($name)
    {
        $httpMessage = [
            "auth" => [$this->options->getUsername(), $this->options->getPassword()],
        ];
         
        return $this->postRequest($httpMessage, 'DELETE');
    }
}
