package ai.basic.x1.usecase;

import ai.basic.x1.adapter.port.dao.ModelDAO;
import ai.basic.x1.adapter.port.dao.mybatis.model.Model;
import ai.basic.x1.entity.ModelBO;
import ai.basic.x1.util.DefaultConverter;
import cn.hutool.core.util.ObjectUtil;
import com.baomidou.mybatisplus.core.conditions.query.LambdaQueryWrapper;
import com.baomidou.mybatisplus.core.toolkit.Wrappers;
import org.springframework.beans.factory.annotation.Autowired;

import java.util.List;

/**
 * @author chenchao
 * @date 2022/8/26
 */
public class ModelUseCase {

    @Autowired
    private ModelDAO modelDAO;

    public List<ModelBO> findAll(ModelBO modelBO){
        LambdaQueryWrapper<Model> modelLambdaQueryWrapper = Wrappers.lambdaQuery();
        modelLambdaQueryWrapper.eq(ObjectUtil.isNotNull(modelBO.getDatasetType()), Model::getDatasetType, modelBO.getDatasetType());
        modelLambdaQueryWrapper.orderBy(true, true, Model::getName);
        var modelList = modelDAO.list(modelLambdaQueryWrapper);
        return DefaultConverter.convert(modelList, ModelBO.class);
    }

    public ModelBO findById(Long id) {
        var model = modelDAO.getById(id);
        return DefaultConverter.convert(model, ModelBO.class);
    }
}