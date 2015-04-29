/*******************************************************************************
 * Copyright (c) 2014 itemis AG and others.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *
 * Contributors:
 *     Matthias Wienand (itemis AG) - initial API & implementation
 *
 *******************************************************************************/
package com.typeassimilation.example;

import org.eclipse.gef4.mvc.parts.IFeedbackPartFactory;
import org.eclipse.gef4.zest.fx.ZestFxModule;

import com.google.inject.TypeLiteral;

import javafx.scene.Node;

public class ZestGraphExampleModule extends ZestFxModule {

	@Override
	protected void bindIFeedbackPartFactory() {
		binder().bind(new TypeLiteral<IFeedbackPartFactory<Node>>() {
		}).toInstance(new ZestGraphExampleFeedbackPartFactory());
	}

}
