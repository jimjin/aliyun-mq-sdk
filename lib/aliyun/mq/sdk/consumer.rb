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
      @time = opts[:time] || (Time.now.to_f * 1000).to_i
      @topic = opts[:topic] || default_topic
      @num = opts[:num]

      sign = Auth.get_sign(secret_key, topic, consumer_id, @time)
      headers = {"Signature" => sign, "AccessKey" => access_key, "ConsumerID" => consumer_id, "Content-Type" => 'text/html;charset=UTF-8'}

      query = {"topic" => topic, "time" => @time}

      query["num"] = @num if @num

      res = self.class.get(region_url, headers: headers, query: query)
      if res.parsed_response
        rslt = {success: true, items: Utils.deep_symbolize_keys(JSON.parse(res.parsed_response))}
      else
        rslt = {success: false, msg: res.response}
      end
      rslt
    end

    def delete(msg_handle, opts={})
      @time = opts[:time] || (Time.now.to_f * 1000).to_i
      @topic = opts[:topic] || default_topic

      sign = Auth.del_sign(secret_key, topic, consumer_id, msg_handle, @time)
      headers = {"Signature" => sign, "AccessKey" => access_key, "ConsumerID" => consumer_id, "Content-Type" => 'text/html;charset=UTF-8'}

      query = {"topic" => topic, "msgHandle" => msg_handle, "time" => @time}

      res = self.class.delete(region_url, headers: headers, query: query)
      if res.code === 204
        rslt = {success: true}
      else
        if res.parsed_response
          rslt = Utils.symbolize_keys(JSON.parse(res.parsed_response).merge(success: false))
        else
          rslt = {success: false, msg: res.response}
        end
      end
      p res
      p rslt
      rslt
    end
  end
end