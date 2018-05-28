package cn.just.spark.core;
/**
 * 二次排序实现key
 * 必须实现Ordered<DoubleSortKey>,Serializable这两个接口
 * @author shinelon
 *
 */

import java.io.Serializable;

import scala.math.Ordered;

public class DoubleSortKey implements Ordered<DoubleSortKey>,Serializable{
	private static final long serialVersionUID = 1L;
	public int frist;
	public int second;
	
	
	public DoubleSortKey(int frist, int second) {
		super();
		this.frist = frist;
		this.second = second;
	}
	/**
	 * 大于
	 */
	@Override
	public boolean $greater(DoubleSortKey other) {
		if(this.frist>other.frist) {
			return true;
		}else if(this.frist==other.frist&&this.second>other.second){
			return true;
		}
		return false;
	}
	/**
	 * 大于等于
	 */
	@Override
	public boolean $greater$eq(DoubleSortKey other) {
		if(this.frist>other.frist) {
			return true;
		}else if(this.frist==other.frist&&(this.second==other.second||this.second>other.second)){
			return true;
		}
		return false;
	}
	/**
	 * 小于
	 */
	@Override
	public boolean $less(DoubleSortKey other) {
		if(this.frist<other.frist) {
			return true;
		}else if(this.frist==other.frist&&this.second<other.second) {
			return true;
		}
		return false;
	}
	@Override
	public boolean $less$eq(DoubleSortKey other) {
		if(this.frist<other.frist) {
			return true;
		}else if(this.frist==other.frist&&(this.second==other.second||this.second<other.second)) {
			return true;
		}
		return false;
	}
	@Override
	public int compare(DoubleSortKey other) {
		if(this.frist-other.frist!=0) {
			return this.frist-other.frist;
		}else {
			return this.second-other.second;
		}
	}
	@Override
	public int compareTo(DoubleSortKey other) {
		if(this.frist-other.frist!=0) {
			return this.frist-other.frist;
		}else {
			return this.second-other.second;
		}
	}
	public int getFrist() {
		return frist;
	}
	public void setFrist(int frist) {
		this.frist = frist;
	}
	public int getSecond() {
		return second;
	}
	public void setSecond(int second) {
		this.second = second;
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + frist;
		result = prime * result + second;
		return result;
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		DoubleSortKey other = (DoubleSortKey) obj;
		if (frist != other.frist)
			return false;
		if (second != other.second)
			return false;
		return true;
	}
}
