package org.openmrs.module.eptsmozart2.api.dao;

import org.hibernate.Criteria;
import org.hibernate.SessionFactory;
import org.hibernate.criterion.Projections;
import org.hibernate.criterion.Restrictions;
import org.openmrs.User;
import org.openmrs.module.eptsmozart2.Mozart2Generation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * @uthor Willa Mhawila<a.mhawila@gmail.com> on 8/17/22.
 */
@Repository(Mozart2GenerationDao.BEAN_NAME)
public class Mozart2GenerationDao {
	
	public static final String BEAN_NAME = "eptsmozart2.mozart2GenerationDao";
	
	protected final Logger LOGGER = LoggerFactory.getLogger(getClass());
	
	private SessionFactory sessionFactory;
	
	@Autowired
	public void setSessionFactory(SessionFactory sessionFactory) {
		this.sessionFactory = sessionFactory;
	}
	
	public Mozart2Generation save(@NotNull final Mozart2Generation mozart2Generation) {
		sessionFactory.getCurrentSession().saveOrUpdate(mozart2Generation);
		return mozart2Generation;
	}
	
	public Mozart2Generation getMozart2GenerationById(Integer id) {
		return (Mozart2Generation) sessionFactory.getCurrentSession().createCriteria(Mozart2Generation.class)
		        .add(Restrictions.eq("id", id));
	}
	
	public Integer getMozart2GenerationCount(User executor) {
		Criteria criteria = createCriteria(executor);
		
		criteria.setProjection(Projections.rowCount());
		
		return ((Number) criteria.uniqueResult()).intValue();
	}
	
	public List<Mozart2Generation> getMozart2Generations(User executor, Integer startIndex, Integer pageSize) {
		Criteria criteria = createCriteria(executor);
		
		if (startIndex != null) {
			criteria.setFirstResult(startIndex);
		}
		
		if (pageSize != null) {
			criteria.setMaxResults(pageSize);
		}
		
		return criteria.list();
	}
	
	private Criteria createCriteria(User executor) {
		Criteria criteria = sessionFactory.getCurrentSession().createCriteria(Mozart2Generation.class);
		
		if (executor != null) {
			criteria.add(Restrictions.eq("executor", executor));
		}
		
		return criteria;
	}
}
