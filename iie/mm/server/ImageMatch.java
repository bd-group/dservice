package iie.mm.server;

import java.awt.image.BufferedImage;
import java.awt.image.ColorModel;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.Scanner;

import javax.imageio.ImageIO;

/**
 * 感觉效果不好，所以改用ImagePHash类来计算hash值
 * 只用到了这个类里的几个方法
 * @author zhaoyang
 *
 */
public class ImageMatch {
	/**
	 * 图片缩小后的宽
	 */
	public static final int FWIDTH = 8;
	/**
	 *  图片缩小后的高
	 */
	public static final int FHEIGHT = 8;
	
	/**
	 * 得到的是16位的16进制的hashcode
	 * @param img
	 * @return
	 */
	public static String getPHashcode(BufferedImage img)
	{
		int w = img.getWidth();
		int h = img.getHeight();
		int pix[] = new int[w * h];
		pix = img.getRGB(0, 0, w, h, pix, 0, w);
		
		pix = shrink(pix, w, h, 32, 32);
		pix = grayImage(pix, 32, 32);
		int[] dctPix = DCT(pix, 32);
		int avrPix = averageGray(dctPix, FWIDTH, FHEIGHT);
		StringBuilder sb = new StringBuilder();
		for(int i=0; i<FHEIGHT; i++) {			
			for(int j=0; j<FWIDTH; j++) {		
				if(dctPix[i*FWIDTH + j] >= avrPix) {
					sb.append("1");	
				} else {
					sb.append("0");	
				}
			}
		}
//		System.out.println(sb.toString());
		
		return binToHex(sb.toString());
	}
	
	public static BufferedImage readImage(byte[] b, int offset, int length) throws IOException
	{
		ByteArrayInputStream in = new ByteArrayInputStream(b, offset, length);    
		BufferedImage image = ImageIO.read(in);     
		return image;
	}
	public static BufferedImage readImage(byte[] b) throws IOException
	{
		ByteArrayInputStream in = new ByteArrayInputStream(b);    
		BufferedImage image = ImageIO.read(in);    
		return image;
	}
	
	/**
	 * 计算汉明距离
	 * @param s1 指纹数1
	 * @param s2 指纹数2
	 * @return 汉明距离
	 */
	public static int distance(String s1, String s2) {
		int count = 0;
//		System.out.println("in distance, s1:"+s1);
//		System.out.println("in distance, s2:"+s2);
		for(int i=0; i<s1.length(); i++) {
			if(s1.charAt(i) != s2.charAt(i)) {
				count ++;
			}
		}
		return count;
	}
	
	/**
	 * 局部均值的图像缩小
	 * 
	 * @param pix 图像的像素矩阵
	 * @param w 原图像的宽
	 * @param h 原图像的高
	 * @param m 缩小后图像的宽
	 * @param n 缩小后图像的高
	 * @return
	 */
	private static int[] shrink(int[] pix, int w, int h, int m, int n) {
		float k1 = (float) m / w;
		float k2 = (float) n / h;
		int ii = (int) (1 / k1); // 采样的行间距
		int jj = (int) (1 / k2); // 采样的列间距
		int dd = ii * jj;
		// int m=0 , n=0;
		// int imgType = img.getType();
		int[] newpix = new int[m * n];

		for (int j = 0; j < n; j++) {
			for (int i = 0; i < m; i++) {
				int r = 0, g = 0, b = 0;
				ColorModel cm = ColorModel.getRGBdefault();
				for (int k = 0; k < jj; k++) {
					for (int l = 0; l < ii; l++) {
						r = r + cm.getRed(pix[(jj * j + k) * w + (ii * i + l)]);
						g = g + cm.getGreen(pix[(jj * j + k) * w + (ii * i + l)]);
						b = b + cm.getBlue(pix[(jj * j + k) * w	+ (ii * i + l)]);
					}
				}
				r = r / dd;
				g = g / dd;
				b = b / dd;
				newpix[j * m + i] = 255 << 24 | r << 16 | g << 8 | b;
				// 255<<24 | r<<16 | g<<8 | b 这个公式解释一下，颜色的RGB在内存中是
				// 以二进制的形式保存的，从右到左1-8位表示blue，9-16表示green，17-24表示red
				// 所以"<<24" "<<16" "<<8"分别表示左移24,16,8位

				// newpix[j*m + i] = new Color(r,g,b).getRGB();
			}
		}
		return newpix;
	}
	
	/**
	 *  将图片转化成黑白灰度图片
	 * @param pix 保存图片像素
	 * @param iw 二维像素矩阵的宽
	 * @param ih 二维像素矩阵的高
	 * @return 灰度图像矩阵
	 */
	private static int[] grayImage(int pix[], int w, int h) {
		//int[] newPix = new int[w*h];
		ColorModel cm = ColorModel.getRGBdefault();
		for(int i=0; i<h; i++) {
			for(int j=0; j<w; j++) {
				//0.3 * c.getRed() + 0.58 * c.getGreen() + 0.12 * c.getBlue()				
//				pix[i*w + j] = (int) (0.3*cm.getRed(pix[i*w + j]) + 0.58*cm.getGreen(pix[i*w + j]) + 0.12*cm.getBlue(pix[i*w + j]) );
				pix[i*w + j] = (cm.getRed(pix[i*w + j]) + cm.getGreen(pix[i*w + j]) + cm.getBlue(pix[i*w + j]) )/3;
			}
		}
		return pix;
	}
	
	   private static double[][] applyDCT(double[][] f, int size) {
		   double[] c = new double[size];
	       
	       for (int i=1;i<size;i++) {
	           c[i]=1;
	       }
	       c[0]=1/Math.sqrt(2.0);
	       int N = size;
	       double[][] F = new double[N][N];
	       for (int u=0;u<N;u++) {
	         for (int v=0;v<N;v++) {
	           double sum = 0.0;
	           for (int i=0;i<N;i++) {
	             for (int j=0;j<N;j++) {
	               sum+=Math.cos(((2*i+1)/(2.0*N))*u*Math.PI)*Math.cos(((2*j+1)/(2.0*N))*v*Math.PI)*(f[i][j]);
	             }
	           }
	           sum*=((c[u]*c[v])/4.0);
	           F[u][v] = sum;
	         }
	       }
	       return F;
	   }
	
	
	/**
	 * 离散余弦变换
	 * @param pix 原图像的数据矩阵
	 * @param n 原图像(n*n)的高或宽
	 * @return 变换后的矩阵数组
	 */
	private static int[] DCT(int[] pix, int n) {		
		double[][] iMatrix = new double[n][n]; 
		for(int i=0; i<n; i++) {
			for(int j=0; j<n; j++) {
				iMatrix[i][j] = (double)(pix[i*n + j]);
			}
		}
		/*
		double[][] tmp = applyDCT(iMatrix, n);
		int[] newpix = new int[n*n];
		for(int i=0; i<n; i++) {
			for(int j=0; j<n; j++) {
				newpix[i*n + j] = (int)tmp[i][j];
			}
		}
		return newpix;
		*/
		double[][] quotient = coefficient(n);	//求系数矩阵
		double[][] quotientT = transposingMatrix(quotient, n);	//转置系数矩阵
		
		double[][] temp = new double[n][n];
		temp = matrixMultiply(quotient, iMatrix, n);
		iMatrix =  matrixMultiply(temp, quotientT, n);
		
		int newpix[] = new int[n*n];
		for(int i=0; i<n; i++) {
			for(int j=0; j<n; j++) {
				newpix[i*n + j] = (int)iMatrix[i][j];
			}
		}
		return newpix;
	}
	
	/**
	 * 求离散余弦变换的系数矩阵
	 * @param n n*n矩阵的大小
	 * @return 系数矩阵
	 */
	private static double[][] coefficient(int n) {
		double[][] coeff = new double[n][n];
		double sqrt = 1.0/Math.sqrt(n);
		for(int i=0; i<n; i++) {
			coeff[0][i] = sqrt;
		}
		for(int i=1; i<n; i++) {
			for(int j=0; j<n; j++) {
				coeff[i][j] = Math.sqrt(2.0/n) * Math.cos(i*Math.PI*(j+0.5)/(double)n);
			}
		}
		return coeff;
	}
	
	/**
	 * 矩阵相乘
	 * @param A 矩阵A
	 * @param B 矩阵B
	 * @param n 矩阵的大小n*n
	 * @return 结果矩阵
	 */
	private static double[][] matrixMultiply(double[][] A, double[][] B, int n) {
		double nMatrix[][] = new double[n][n];
		double t = 0.0;
		for(int i=0; i<n; i++) {
			for(int j=0; j<n; j++) {
				t = 0;
				for(int k=0; k<n; k++) {
					t += A[i][k]*B[k][j];
				}
				nMatrix[i][j] = t;			}
		}
		return nMatrix;
	}
	
	/**
	 * 矩阵转置
	 * @param matrix 原矩阵
	 * @param n 矩阵(n*n)的高或宽
	 * @return 转置后的矩阵
	 */
	private static double[][]  transposingMatrix(double[][] matrix, int n) {
		double nMatrix[][] = new double[n][n];
		for(int i=0; i<n; i++) {
			for(int j=0; j<n; j++) {
				nMatrix[i][j] = matrix[j][i];
			}
		}
		return nMatrix;
	}
	
	/**
	 * 求灰度图像的均值
	 * @param pix 图像的像素矩阵
	 * @param w 图像的宽
	 * @param h 图像的高
	 * @return 灰度均值
	 */
	private static int averageGray(int[] pix, int w, int h) {
		int sum = 0;
		for(int i=0; i<h; i++) {
			for(int j=0; j<w; j++) {
				sum = sum+pix[i*w + j];
			}
			
		}
		sum -= pix[0];
		return (int)(sum/(w*h - 1));
	}
	
	/**
	 * 把二进制的字符串转换成16进制形式，bi长度得是4的倍数
	 * @param bi
	 * @return
	 */
	public static String binToHex(String bi)
	{
		int len = bi.length();
		String hex = "";
		for(int i = len -1; i > 0;i -= 4)
		{
			String sub = bi.substring(i-3 >= 0? i-3:0, i+1);
			String a = Integer.toHexString(Integer.parseInt(sub,2));
			hex = a + hex;
		}
		return hex;
	}
	
	/**
	 * 十六进制字符串转换成二进制，每个十六进制字符转换成4位二进制
	 * @param hex
	 * @return
	 */
	public static String hexToBin(String hex)
	{
		String bin = "";
		for(int i = 0;i<hex.length();i++)
		{
			String s = Integer.toBinaryString(Integer.parseInt(hex.charAt(i)+"",16));
			while(s.length() < 4)
				s = "0"+s;
			bin += s;
		}
		return bin;
	}
	
	public static void main(String[] a) throws IOException
	{
		File dir = new File("/home/zhaoyang/workspace_javaee/ImageSearch/src/image");
		FileInputStream fis = new FileInputStream("/home/zhaoyang/picture/image/person.jpg");
		byte[] c = new byte[fis.available()];
		fis.read(c);
		String sr = getPHashcode(readImage(c));
		System.out.println(sr);
//		Scanner sc = new Scanner(System.in);
//		sc.next();
		long start = System.currentTimeMillis();
		for(File f : dir.listFiles())
		{
			try {
				fis = new FileInputStream(f);
				byte[] content = new byte[fis.available()];
				fis.read(content);
				BufferedImage b = readImage(content);
				String h = getPHashcode(b);
//				if(sr.equals(""))
//					sr = h;
				System.out.println(f.getName()+":"+h+"\t"+distance(hexToBin(sr), hexToBin(h)));
//				System.out.println(hexToBin(h));
//				System.out.println(h.equals(binToHex(hexToBin(h))));
//				System.out.println(sp.put("im@"+f.getName(), content));
			} catch (FileNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}finally{
				fis.close();
			}
		}
		System.out.println("time:"+(System.currentTimeMillis()-start));
	}
}
