package net.floodlightcontroller.qoedrivenadjustment;

import net.floodlightcontroller.core.module.Run;
import org.projectfloodlight.openflow.types.IPv4Address;
import sun.misc.BASE64Encoder;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Inet4Address;
import java.net.ServerSocket;
import java.net.Socket;
import java.security.MessageDigest;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by ningjieqian on 17/7/18.
 */
public class ComplainCollecter implements Runnable{
    private List<IPv4Address> unsatClient;

    public ComplainCollecter(List<IPv4Address> unsatClient){
        this.unsatClient = unsatClient;
    }

    public void run(){
        System.out.println("server start");
        ExecutorService executor = Executors.newCachedThreadPool();
        try(ServerSocket ss = new ServerSocket(30000)){
            while(true){
                Socket socket = ss.accept();
                System.out.println(socket.getInetAddress() + " connected");
                executor.execute(new Collect(socket));
            }
        }catch(IOException e){
            e.printStackTrace();
        }
    }

    private class Collect implements Runnable{
        private final Socket socket;

        public Collect(Socket socket){
            this.socket = socket;
        }

        public void run(){
            try{
                try{
                    shakeHand();

                    while(true){
                        String msg = receive();
                        System.out.println("bufferLevel = " + msg);
                        try{
                            Double bufferLevel = Double.parseDouble(msg);
                            Inet4Address ipv4 = (Inet4Address) socket.getInetAddress();
                            unsatClient.add(IPv4Address.of(ipv4.getAddress()));
                        } catch(NumberFormatException e){
                            System.out.println("why??");
                        }

                    }
                }finally{
                    socket.close();
                }
            }catch(Exception e){
                e.printStackTrace();
            }
        }

        private void shakeHand() throws Exception{
            InputStream in = socket.getInputStream();
            OutputStream out = socket.getOutputStream();
            byte[] buff = new byte[1024];
            int count = -1;
            String req = "";
            count = in.read(buff);
            req = new String(buff, 0, count);
            System.out.println("握手请求：" + req);

            String secKey = getSecWebSocketKey(req);
            System.out.println("secKey = " + secKey);
            String response = "HTTP/1.1 101 Switching Protocols\r\nUpgrade: "
                    + "websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "
                    + getSecWebSocketAccept(secKey) + "\r\n\r\n";
            System.out.println("secAccept = " + getSecWebSocketAccept(secKey));
            out.write(response.getBytes());
        }

        private String receive() throws Exception{
            InputStream in = socket.getInputStream();
            byte[] buff = new byte[1024];
            int count = -1;
            count = in.read(buff);

            for(int i = 0;i < count - 6;i++){
                buff[i+6] = (byte)(buff[i%4 + 2] ^ buff[i+6]);
            }
            return new String(buff, 6, count - 6, "UTF-8");
        }

		/*
		private void send(String msg) throws Exception{
			OutputStream out = socket.getOutputStream();
	        byte[] pushHead = new byte[2];
	        pushHead[0] = -127;
	        pushHead[1] = (byte)msg.getBytes("UTF-8").length;
	        out.write(pushHead);
	        out.write(msg.getBytes("UTF-8"));
		}
		*/

        private String getSecWebSocketKey(String req){
            Pattern p = Pattern.compile("^(Sec-Websocket-Key:).+",
                    Pattern.CASE_INSENSITIVE | Pattern.MULTILINE);
            Matcher m = p.matcher(req);
            if(m.find()){
                String foundstring = m.group();
                return foundstring.split(":")[1].trim();
            }else{
                return null;
            }
        }

        private String getSecWebSocketAccept(String secKey) throws Exception{
            String guid = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";

            secKey += guid;

            MessageDigest md = MessageDigest.getInstance("SHA-1");
            md.update(secKey.getBytes("ISO-8859-1"),0,secKey.length());

            byte[] sha1Hash = md.digest();

            BASE64Encoder encoder = new BASE64Encoder();
            return encoder.encode(sha1Hash);
        }

    }

}
