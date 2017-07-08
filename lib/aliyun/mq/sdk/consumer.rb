module Aliyun::Mq::Sdk
  class Consumer
    include HTTParty

    DEFAULT_BASE_URI = 'http://publictest-rest.ons.aliyun.com/message/'

    attr_accessor :access_key, :secret_key, :consumer_id, :region_url, :default_topic, :topic

    def initialize(access_key, secret_key, consumer_id, opts={})
      @access_key = access_key
      @secret_key = secret_key
      @consumer_id = consumer_id

      @region_url = opts[:region_url] || DEFAULT_BASE_URI
      @default_topic = opts[:default_topic]
    end

    def receive(opts={})
      @time = Time.now.to_i * 1000
      @topic = opts[:topic] || default_topic
      @num = opts[:num]
      @sharding_key = opts[:sharding_key]

      sign = Auth.get_sign(secret_key, topic, consumer_id, @time)
      headers = {"Signature" => sign, "AccessKey" => access_key, "ConsumerID" => consumer_id, "Content-Type" => 'text/html;charset=UTF-8'}

      query = {"topic" => topic, "time" => @time}

      query["num"] = @num if @num
      query["shardingKey"] = @sharding_key if @sharding_key

      res = self.class.get(region_url, headers: headers, query: query)
      if res.parsed_response
        rslt = {success: true, items: JSON.parse(res.parsed_response)}
      else
        rslt = {success: false, msg: res.response}
      end
      p res
      p rslt
      rslt
    end

    def delete()
    end
  end
end