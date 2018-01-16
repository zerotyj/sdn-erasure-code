package restapi;

import java.io.IOException;
import java.io.StringWriter;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;
import org.apache.commons.httpclient.methods.StringRequestEntity;
import org.codehaus.jackson.map.ObjectMapper;

public class RestClient {
	private final ObjectMapper jsonMapper;
	private final HttpClient sdnClient;
	private final String URL;

	public RestClient(String url) {
		this.URL = url;
	    jsonMapper = new ObjectMapper();
	    sdnClient = new HttpClient();
	}
	    
	public <T extends Object> T doRestCall(String path, Object req, Class<T> valueType) 
	        throws IOException {
		String reqStr = convertObjectToJson(req);
	    String respStr = doRestCall(path, reqStr);
	    T resp = convertJsonToObject(respStr, valueType);
	    return resp;
	}

	public  String doRestCall(String path, String reqStr) throws IOException {
		PostMethod post = new PostMethod(URL + path);
		StringRequestEntity reqEntity = new StringRequestEntity(reqStr, "application/json", "UTF-8");
		post.setRequestEntity(reqEntity);
		sdnClient.executeMethod(post);
		String status = post.getStatusText();
		String response = post.getResponseBodyAsString();
		System.out.println(String.format("Response: %s : %s", status, response));
		return response;
	}

	// 将对象转化为String
	public  String convertObjectToJson(Object req) throws IOException {
	    StringWriter writer = new StringWriter();
	    jsonMapper.writeValue(writer, req);
	    return writer.toString();
	}

	// 将String转化为对象
	public  <T extends Object> T convertJsonToObject(String content, Class<T> valueType)
				throws IOException {
	    return jsonMapper.readValue(content, valueType);
	}
}
