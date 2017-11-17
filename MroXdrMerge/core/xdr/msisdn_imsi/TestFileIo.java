package xdr.msisdn_imsi;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;

public class TestFileIo {
	
	
    public static void main(String[] args) {
    	  FileInputStream fis = null;
          InputStreamReader isr = null;
          BufferedReader br = null ;//用于包装InputStreamReader,提高处理性能。因为BufferedReader有缓冲的，而InputStreamReader没有。
          String str ="";
          String str1="";
          StringBuffer sb=new StringBuffer();
          
          try {
              fis = new FileInputStream("G:\\gaotie\\haerbin\\peizhi\\cell1.txt");
              isr = new InputStreamReader(fis);
              br = new BufferedReader(isr);// 从字符输入流中读取文件中的内容,封装了一个new InputStreamReader的对象
              while ((str = br.readLine())!=null){
            	  str = str.replace(",", "\t");
            	  String[] splits =str.split("\t");
            	  try{
            		  int ilong = new Double(Double.parseDouble(splits[1])*10000000+"").intValue();
            		  int ilat = new Double(Double.parseDouble(splits[2])*10000000+"").intValue();
//            		  
//            		  sb.append(ilong+"\t"+ilat+"\n");
            		  sb.append(ilong/10000000.0+"\t"+ilat/10000000.0+"\n");
            	  }catch(Exception e){
            		  e.printStackTrace();
            	  }
            	  
              }
              System.out.println(str1);

          } catch (Exception e) {
              e.printStackTrace();
          }
    	
    	
        File file=new File("G:\\gaotie\\haerbin\\peizhi\\cellhuitian1.txt");
        if(!file.exists()){
            try {
                file.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        FileOutputStream out= null;
        try {
            out = new FileOutputStream(file,false);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        
        try {
            out.write(sb.toString().getBytes("utf-8"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            out.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    }
