package br.com.dishup.environment;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Scanner;
import org.hibernate.Session;
import org.hibernate.Transaction;
import br.com.dishup.core.entity.EmploymentEntity;
import br.com.dishup.core.exception.EmptyTableException;
import br.com.dishup.core.exception.FileEmptyException;
import br.com.dishup.core.persistence.EmploymentDAO;
import br.com.dishup.core.persistence.HibernateUtil;
import br.com.dishup.core.util.EmploymentComparatorUtil;
import br.com.dishup.core.util.StatisticFileUtil;

public class LoadEmploymentEnvironment {

	public void loadEmployment(String filePath){
		Session session = HibernateUtil.getSessionFactory().openSession();
		Transaction transaction = session.beginTransaction();
		
		File file = new File(filePath);
		
		StatisticFileUtil statistic = new StatisticFileUtil(new LoadEmploymentEnvironment(), "loadEmployment(String filePath)", file, true, "dishup.cargo");
		statistic.start();
		
		ArrayList<EmploymentEntity> listFile = new ArrayList<EmploymentEntity>();
		EmploymentDAO employmentDAO = new EmploymentDAO();
		
		try {
			listFile = loadFileIntoArray(file);
			Collections.sort(listFile, new EmploymentComparatorUtil());
			statistic.setNumberOfRegisterFile(listFile.size());
			List<EmploymentEntity> listBase = new ArrayList<EmploymentEntity>();
			try{
				listBase = employmentDAO.selectAllOrderById(session);
				statistic.setNumberOfRegisterBase(listBase.size());
				int countListBase = 0, countListFile = 0, numberOfRegisterBase = listBase.size(), numberOfRegisterFile = listFile.size();
				while((countListBase < numberOfRegisterBase) || (countListFile < numberOfRegisterFile)){
					if((countListBase < numberOfRegisterBase) && (countListFile < numberOfRegisterFile)){
						if(listFile.get(countListFile).getId() == listBase.get(countListBase).getId()){
							countListBase++;
							countListFile++;
						}else if(listFile.get(countListFile).getId() > listBase.get(countListBase).getId()){
							employmentDAO.deleteById(session,listBase.get(countListBase).getId());
							statistic.incrementRegisterDeleted();
							countListBase++;
						}else{
							employmentDAO.insert(session,listFile.get(countListFile));
							statistic.incrementRegisterWrite();
							countListFile++;
						}
					}else if((countListBase < numberOfRegisterBase) && (countListFile >= numberOfRegisterFile)){
						employmentDAO.deleteById(session,listBase.get(countListBase).getId());
						statistic.incrementRegisterDeleted();
						countListBase++;
					}else if((countListBase >= numberOfRegisterBase) && (countListFile < numberOfRegisterFile)){
						employmentDAO.insert(session,listFile.get(countListFile));
						statistic.incrementRegisterWrite();
						countListFile++;
					}
				}
			}catch(EmptyTableException e){
				statistic = new StatisticFileUtil(new LoadEmploymentEnvironment(), "loadEmployment(String filePath)", file, false, "");
				statistic.start();
				statistic.setNumberOfRegisterFile(listFile.size());
				for(int i = 0; i < listFile.size(); i++){
					try{
						statistic.incrementRegisterRead();
						employmentDAO.insert(session,listFile.get(i));
						statistic.incrementRegisterWrite();
					}catch(Exception e1){
						e1.printStackTrace();
					}
				}
			}
			
		}catch(Throwable e){
			e.printStackTrace();
		}
		transaction.commit();
		session.close();
		statistic.end();
		System.out.println(statistic.toString());
	}
	
	private ArrayList<EmploymentEntity> loadFileIntoArray(File file) throws FileNotFoundException, FileEmptyException{
		ArrayList<EmploymentEntity> list = new ArrayList<EmploymentEntity>();
		Scanner scanner = new Scanner(file);
		String var = "";
		String[] parms;
		if(!scanner.hasNext())
			throw new FileEmptyException("FILE (PATH: "+file.getPath()+" ) IS EMPTY");
		while(scanner.hasNext()){
			try{
				var = scanner.nextLine();
				parms = var.split(";");
				list.add(new EmploymentEntity(Integer.valueOf(parms[0]), parms[1], parms[2]));
			}catch(Exception e){
				e.printStackTrace();
			}
		}
		scanner.close();
		return list;
	}
}